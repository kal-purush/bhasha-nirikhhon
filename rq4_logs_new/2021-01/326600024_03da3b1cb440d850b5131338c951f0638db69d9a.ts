import { PromoEnrollment as PromoEnrollmentModel } from '../../../lib/mongoose/models/promo-enrollment';
import { PromoEnrollmentsStore } from '../../../types/index';

const actions = ({
  PromoEnrollment,
}: {
  PromoEnrollment: typeof PromoEnrollmentModel;
}): PromoEnrollmentsStore => {
  async function insertPromoEnrollment(info) {
    return PromoEnrollment.create(info);
  }

  async function promoEnrollmentExistsByFilter(filters) {
    return PromoEnrollment.exists(filters);
  }

  async function selectOnePromoEnrollmentByFilters(filters) {
    return PromoEnrollment.findOne(filters)
      .populate('member')
      .populate('promo')
      .lean({ virtuals: true });
  }

  return {
    insertPromoEnrollment,
    promoEnrollmentExistsByFilter,
    selectOnePromoEnrollmentByFilters,
  };
};

export default actions;