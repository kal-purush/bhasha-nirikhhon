import prisma from "@youmeet/prisma-config/prisma";
import reduceAppelations from "@youmeet/utils/resolvers/reduceAppelations";

(async () => {
  const offers = await prisma.offers.findMany();
  for (let i = 0; i < offers.length; i++) {
    const offer = offers[i];
    let intituleReduced = offer.intituleReduced;
    let romeLibelleReduced = offer.romeLibelleReduced;
    const tuple = reduceAppelations(offer);
    let modified = false;
    if (!intituleReduced) {
      intituleReduced = tuple[0];
      modified = true;
    }
    if (!romeLibelleReduced) {
      romeLibelleReduced = tuple[1];
      modified = true;
    }
    if (modified) {
      try {
        await prisma.offers.update({
          where: { id: offer.id },
          data: { intituleReduced, romeLibelleReduced },
        });
        console.log(
          `Appelations ${intituleReduced} ${romeLibelleReduced} added to offer ${offer.id}`
        );
      } catch (e) {
        console.error(e);
      }
    }
  }
  process.exit(0);
})();