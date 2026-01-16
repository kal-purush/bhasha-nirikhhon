import prisma from "@/lib/prisma";
import {
  BaseMatchup,
  DbPickWithOdds,
  IPick,
  ParlayWithPicksAndOdds,
  PickHistory,
} from "@/lib/types/interfaces";

export const getActiveMatchups = async (matchupIds: string[]): Promise<BaseMatchup[]> => {
  return await prisma.matchups.findMany({
    where: {
      id: { in: matchupIds },
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
};

export const getActivePicks = (
  existingParlay: ParlayWithPicksAndOdds,
  activeMatchups: BaseMatchup[]
): IPick[] => {
  if (Object.keys(existingParlay).length === 0) {
    return [];
  }

  return existingParlay.picks.map(pick => {
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
};

export const getPickHistory = (
  allParlayPicks: DbPickWithOdds[],
  activeMatchups: BaseMatchup[]
): PickHistory[] => {
  if (!allParlayPicks.length) return [];
  return allParlayPicks.map(pick => {
    const { matchupId, result } = pick;
    const matchup = activeMatchups.find(({ id }) => id === matchupId);
    if (!matchup) {
      throw new Error("Matchup not found");
    }
    return {
      matchupId,
      pick: pick.pick,
      result,
    };
  });
};