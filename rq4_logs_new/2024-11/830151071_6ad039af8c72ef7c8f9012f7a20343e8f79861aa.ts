import { getOffersFT } from "@youmeet/functions/request";
import prisma from "@youmeet/prisma-config/prisma";
import { OffreEmploiFT } from "@youmeet/types/api/OffreEmploiFT";
import { setUniqueSlugAndExtension } from "@youmeet/utils/backoffice/setUniqueInput";

(async () => {
  let start = 0;
  let end = 0;
  let over = start > 149 || end >= 149;
  type ResultOffresFT = { resultats: OffreEmploiFT[] };

  const waitFor = (ms: number) => new Promise((r) => setTimeout(r, ms));

  const fnc = async (start: number, end: number, over: boolean) => {
    if (over) return;
    end = start + 149;
    console.log(`Fetching offers from ${start} to ${end}`);
    const result = (await getOffersFT<ResultOffresFT>({
      range: `${start}-${end}`,
    })) as ResultOffresFT;
    console.log(`Fetched: ${result}`);
    const data = result?.resultats;
    const offres = data?.length > 0 ? data : [];

    for (let i = 0; i < offres.length; i++) {
      const FToffer = offres[i];

      const offer = Object.fromEntries(
        Object.entries(FToffer).map(([key, val]) => {
          if (typeof val === "object") return [key, { set: val }];
          return [key, val];
        })
      );
      delete offer.id;

      const { extension, slug } = await setUniqueSlugAndExtension(
        offer.romeLibelle,
        1,
        "offers"
      );

      await prisma.offers.create({
        data: {
          ...offer,
          createdAt: new Date(),
          updatedAt: new Date(),
          extension,
          slug,
        },
      });
    }

    start = end + 1;
    await waitFor(5000);
    await fnc(start, end, start > 149 || end >= 149);
  };

  await fnc(start, end, over);

  process.exit(0);
})();