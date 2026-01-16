import { z } from "zod";
import { createTRPCRouter, privateProcedure } from "~/server/api/trpc";
import * as cheerio from "cheerio";
import axios from "redaxios";
import Jimp from "jimp";
import tinycolor from "tinycolor2";
import { isReadableWhiteFont } from "~/utils/helpers";

export type RosterPlayerType = {
  name: string;
  number: number;
  isGoalkeeper: boolean;
  isCaptain: boolean;
  id: number;
};

export const playersRouter = createTRPCRouter({
  getTeamRoster: privateProcedure
    .input(
      z.object({
        rosterUrl: z.string(),
      })
    )
    .query(async ({ input }) => {
      const { data }: { data: string } = await axios(input.rosterUrl);
      const $ = cheerio.load(data);
      const roster: RosterPlayerType[] = [];
      $('tbody[id="rosterListingTableBodyPlayer"] > tr').each(function (id) {
        const name = $(this).find('td[class="name"] > a').text();
        roster.push({
          name: name.replace(/^\d[^A-Za-z]*/, ""),
          number: 0,
          isGoalkeeper: false,
          isCaptain: false,
          id,
        });
      });
      return roster;
    }),

  createTeamXI: privateProcedure
    .input(
      z.object({
        startingXI: z
          .object({
            name: z.string(),
            number: z.number(),
            isGoalkeeper: z.boolean(),
            isCaptain: z.boolean(),
            id: z.number(),
          })
          .array(),
        headCoach: z.string(),
        teamId: z.number(),
        xiGraphic: z.string(),
        hex: z.string(),
        teamName: z.string(),
        coachHex: z.string(),
      })
    )
    .mutation(async ({ input }) => {
      const graphic = await Jimp.read(
        `public/xi-graphics/${input.xiGraphic}.png`
      );
      const publicSansWhite = await Jimp.loadFont(
        "public/jimp-fonts/publicSansWhite.fnt"
      );
      const publicSansBlack = await Jimp.loadFont(
        "public/jimp-fonts/publicSansBlack.fnt"
      );
      const hex = tinycolor(input.hex);
      const coachHex = tinycolor(input.coachHex);

      const font = isReadableWhiteFont(hex) ? publicSansWhite : publicSansBlack;
      const coachFont = isReadableWhiteFont(coachHex)
        ? publicSansWhite
        : publicSansBlack;

      let i = 0;
      let altText = `Starting eleven for ${input.teamName}: `;
      const altTextArray: string[] = [];
      for (const player of input.startingXI) {
        graphic.print(
          font,
          -435,
          i * 45 + 290,
          {
            text: player.number.toString(10),
            alignmentX: Jimp.HORIZONTAL_ALIGN_CENTER,
            alignmentY: Jimp.VERTICAL_ALIGN_TOP,
          },
          1080,
          1080
        );
        graphic.print(
          font,
          140,
          i * 45 + 290,
          {
            text: player.isGoalkeeper ? `${player.name} (GK)` : player.name,
            alignmentX: Jimp.HORIZONTAL_ALIGN_LEFT,
            alignmentY: Jimp.VERTICAL_ALIGN_TOP,
          },
          1080,
          1080
        );
        altTextArray.push(
          player.isGoalkeeper
            ? `#${player.number} ${player.name} (goalkeeper)`
            : `#${player.number} ${player.name}`
        );
        i++;
      }

      graphic.print(
        coachFont,
        90,
        888,
        {
          text: input.headCoach,
          alignmentX: Jimp.HORIZONTAL_ALIGN_LEFT,
          alignmentY: Jimp.VERTICAL_ALIGN_TOP,
        },
        1080,
        1080
      );
      altTextArray.push(`and Head Coach ${input.headCoach}`);

      const base64 = await graphic.getBase64Async(Jimp.AUTO);
      altText += altTextArray.join(", ");

      return { base64, altText };
    }),
});