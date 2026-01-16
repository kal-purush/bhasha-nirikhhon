import { getServerSession } from "next-auth";
import { options } from "../api/auth/[...nextauth]/options";
import prisma from "@/lib/prisma";
import { IPick, ParlayWithPicksAndOdds } from "@/lib/types/interfaces";

export interface GetParlays {
  parlays: ParlayWithPicksAndOdds[];
  pickHistory: IPick[];
  activePoints: number;
  activePicks: IPick[];
  dbActivePicks: IPick[];
  locked: boolean;
  error?: string;
}

export const getParlays = async () => {
  const defaultState: GetParlays = {
    parlays: [],
    pickHistory: [],
    activePoints: 100, // decide on default here
    locked: false,
    activePicks: [],
    dbActivePicks: [],
  };

  try {
    const session = await getServerSession(options);

    if (!session) {
      console.log("no session found");
      return defaultState;
    }

    const user = await prisma.user.findUnique({ where: { email: session?.user?.email ?? "" } });

    if (!user) {
      return { ...defaultState, error: "Error fetching user account" };
    }

    const parlays: ParlayWithPicksAndOdds[] = await prisma.parlay.findMany({
      where: {
        userId: user.id,
        // TODO add time based filters to only get parlays from current weekly round
      },
      orderBy: {
        createdAt: "desc",
      },
      include: {
        picks: {
          orderBy: {
            createdAt: "desc",
          },
          include: {
            odds: true,
          },
        },
      },
    });

    let createdParlay;
    if (!parlays.length) {
      // TODO update default points when handling forfeits
      const DEFAULT_POINTS_WAGERED = 100;
      createdParlay = await prisma.parlay.create({
        data: {
          userId: user.id,
          pointsWagered: DEFAULT_POINTS_WAGERED,
        },
      });
    }

    // TODO tidy up and test this logic
    const existingParlay = parlays?.[0] ?? [];
    const allParlayPicks = parlays.map(parlay => parlay.picks) ?? [];

    if (!parlays.length && !createdParlay) {
      return { ...defaultState, error: "No parlay found" };
    }

    const locked = createdParlay ? false : existingParlay?.locked;
    const activePicksFromDb = existingParlay?.picks ?? [];
    const parlayIsOver =
      !locked &&
      activePicksFromDb.length > 0 &&
      activePicksFromDb.every(pick => ["win", "loss", "push"].includes(pick.result));

    let activePoints;
    if (createdParlay) {
      activePoints = createdParlay?.pointsWagered;
    } else {
      activePoints = parlayIsOver ? existingParlay.pointsAwarded : existingParlay.pointsWagered;
    }

    const activeMatchups = await prisma.matchups.findMany({
      where: {
        id: {
          in: allParlayPicks.flatMap(parlay => parlay.map(({ matchupId }) => matchupId)),
        },
      },
      select: {
        id: true,
        strHomeTeam: true,
        strAwayTeam: true,
        oddsType: true,
        awayBadgeId: true,
        homeBadgeId: true,
      },
    });

    // TODO do this transformation for pickHistory and then copy the picks from first parlay to activePicks
    let activePicks: IPick[] = [];
    const pickHistory: IPick[][] = allParlayPicks.map(parlay => {
      return parlay.map(pick => {
        const { matchupId, oddsId, odds, id, useLatestOdds, result } = pick;
        const matchup = activeMatchups.find(({ id }) => id === matchupId);
        if (!matchup) {
          throw new Error("Matchup not found");
        }
        const { strAwayTeam, awayBadgeId, homeBadgeId, oddsType } = matchup;
        const pickIsAwayTeam = strAwayTeam === pick.pick;

        let pickOdds;
        let badge = "";
        // TODO handle draw odds here
        if (oddsType === "totals") {
          pickOdds = pick.pick === "over" ? odds.overOdds : odds.underOdds;
        } else {
          pickOdds = pickIsAwayTeam ? odds.awayOdds! : odds.homeOdds!;
          badge = pickIsAwayTeam ? awayBadgeId : homeBadgeId;
        }

        if (!pickOdds) {
          throw new Error("Error parsing odds");
        }

        return {
          pickId: id,
          matchupId,
          oddsId,
          pick: pick.pick,
          pickOdds,
          badge,
          oddsType,
          useLatestOdds,
          result,
        };
      });
    });

    if (createdParlay || parlayIsOver) {
      activePicks = [];
    } else {
      activePicks = pickHistory[0];
    }

    return {
      parlays,
      pickHistory: pickHistory.flat(),
      activePoints,
      locked,
      activePicks,
      dbActivePicks: activePicks,
    };
  } catch (error) {
    console.log(error);
    return { ...defaultState, error: "Error fetching parlays" };
  }
};