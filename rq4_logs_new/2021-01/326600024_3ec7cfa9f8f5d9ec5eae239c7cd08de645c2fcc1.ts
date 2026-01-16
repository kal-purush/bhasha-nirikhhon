import { UseCase, PromosStore, PromoStatus } from '../../types';

const deleteOnePromo = ({
  promosStore,
}: {
  promosStore: PromosStore;
}): UseCase<boolean> => {
  return async function useCase({ id }) {
    const promo = await promosStore.selectOnePromoByFilters({ _id: id });

    if (!promo) {
      throw new Error(`Promo not found`);
    }

    if (promo.status === PromoStatus.Active) {
      throw new Error(`Active promos can't be deleted`);
    }

    await promosStore.deleteOnePromo({ _id: id });

    return true;
  };
};

export default deleteOnePromo;