import { UseCase, PromosStore } from '../../types';

const updatePromo = ({
  promoEntity,
  promosStore,
  R,
}: {
  promosStore: PromosStore;
  promoEntity;
  R;
}): UseCase<boolean> => {
  return async function ({ id, info }) {
    const promoExists = await promosStore.promoExistsByFilter({
      _id: id,
    });

    if (!promoExists) {
      throw new Error(`Promo ID doesn't exist`);
    }

    const promo = await promoEntity(info);

    const truthlyPromo = await R.filter(R.complement(R.isNil))(promo);

    await promosStore.updatePromoByFilters({ _id: id }, truthlyPromo);

    return true;
  };
};

export default updatePromo;