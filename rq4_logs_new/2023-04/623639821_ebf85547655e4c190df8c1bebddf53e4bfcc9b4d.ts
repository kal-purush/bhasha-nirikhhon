/* eslint-disable @typescript-eslint/restrict-template-expressions */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { prisma } from "~/server/db";

async function main() {
  console.log("Start seeding men's division and teams...");
  await prisma.division.create({
    data: {
      name: "Men's Division",
      teams: {
        createMany: {
          data: [
            {
              name: "Alliance United FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815394?subseason=875807",
              twitterHandle: "@ALLIANCE_UTDFC",
            },
            {
              name: "Blue Devils FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815395?subseason=875807",
              twitterHandle: "@TheBlueDevilsFC",
            },
            {
              name: "Burlington FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815396?subseason=875807",
              twitterHandle: "@burlsoccer",
            },
            {
              name: "BVB IA Waterloo",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815397?subseason=875807",
              twitterHandle: "@bvbiawaterloo",
            },
            {
              name: "Darby FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815398?subseason=875807",
              twitterHandle: "@DarbyFCL1OM",
            },
            {
              name: "Electric City FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815399?subseason=875807",
              twitterHandle: "@ElectricCityFC",
            },
            {
              name: "FC London",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815400?subseason=875807",
              twitterHandle: "@FCLondon",
            },
            {
              name: "Guelph United",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815401?subseason=875807",
              twitterHandle: "@guelphunitedfc",
            },
            {
              name: "Hamilton United",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815402?subseason=875807",
              twitterHandle: "@HamutdElite",
            },
            {
              name: "Master's FA",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815403?subseason=875807",
              twitterHandle: "@MastersFA",
            },
            {
              name: "North Mississauga SC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815404?subseason=875807",
              twitterHandle: "@NMSC_Panthers",
            },
            {
              name: "North Toronto Nitros",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815405?subseason=875807",
              twitterHandle: "@NT_SoccerClub",
            },
            {
              name: "ProStars FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815407?subseason=875807",
              twitterHandle: "@ProStarsFC",
            },
            {
              name: "Scrosoppi FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815408?subseason=875807",
              twitterHandle: "@FcScrosoppi",
            },
            {
              name: "Sigma FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815409?subseason=875807",
              twitterHandle: "@SigmaFC",
            },
            {
              name: "Simcoe County Rovers FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815410?subseason=875807",
              twitterHandle: "@RoversFC_L1O",
            },
            {
              name: "St. Catherines Roma Wolves",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815411?subseason=875807",
              twitterHandle: "@stcromawolvesL1",
            },
            {
              name: "Unionville Milliken SC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815412?subseason=875807",
              twitterHandle: "@u_msc",
            },
            {
              name: "Vaughan Azzurri",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815413?subseason=875807",
              twitterHandle: "@vaughansoccercl",
            },
            {
              name: "Windsor City FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815414?subseason=875807",
              twitterHandle: "@WinCityFC1",
            },
            {
              name: "Woodbridge Strikers",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815415?subseason=875807",
              twitterHandle: "@WoodbridgeL1OM",
            },
          ],
        },
      },
    },
  });
  console.log("Start seeding women's division and teams...");
  await prisma.division.create({
    data: {
      name: "Women's Division",
      teams: {
        createMany: {
          data: [
            {
              name: "Alliance United FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815431?subseason=875808",
              twitterHandle: "@ALLIANCE_UTDFC",
            },
            {
              name: "Blue Devils FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815432?subseason=875808",
              twitterHandle: "@TheBlueDevilsFC",
            },
            {
              name: "Burlington FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815433?subseason=875808",
              twitterHandle: "@burlsoccer",
            },
            {
              name: "BVB IA Waterloo",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815434?subseason=875808",
              twitterHandle: "@bvbiawaterloo",
            },
            {
              name: "Darby FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815435?subseason=875808",
              twitterHandle: "@DARBYFCL1O",
            },
            {
              name: "Electric City FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815436?subseason=875808",
              twitterHandle: "@ElectricCityFC",
            },
            {
              name: "FC London",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815437?subseason=875808",
              twitterHandle: "@FCLondon",
            },
            {
              name: "Guelph Union",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815438?subseason=875808",
              twitterHandle: "@GuelphUnion",
            },
            {
              name: "Hamilton United",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815439?subseason=875808",
              twitterHandle: "@HamutdElite",
            },
            {
              name: "NDC-Ontario",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815440?subseason=875808",
              twitterHandle: "@NDC_Ontario",
            },
            {
              name: "North Mississauga SC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815441?subseason=875808",
              twitterHandle: "@NMSC_Panthers",
            },
            {
              name: "North Toronto Nitros",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815442?subseason=875808",
              twitterHandle: "@NT_SoccerClub",
            },
            {
              name: "ProStars FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815444?subseason=875808",
              twitterHandle: "@ProStarsFC",
            },
            {
              name: "Simcoe County Rovers FC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815445?subseason=875808",
              twitterHandle: "@RoversFC_L1O",
            },
            {
              name: "St. Catherines Roma Wolves",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815446?subseason=875808",
              twitterHandle: "@stcromawolvesL1",
            },
            {
              name: "Tecumseh SC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815447?subseason=875808",
              twitterHandle: "@TecumsehLeague1",
            },
            {
              name: "Unionville Milliken SC",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815448?subseason=875808",
              twitterHandle: "@u_msc",
            },
            {
              name: "Vaughan Azzurri",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815449?subseason=875808",
              twitterHandle: "@vaughansoccercl",
            },
            {
              name: "Woodbridge Strikers",
              rosterUrl:
                "https://www.league1ontario.com/roster/show/7815450?subseason=875808",
              twitterHandle: "@WoodbridgeL1OW",
            },
          ],
        },
      },
    },
  });
}

main()
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });