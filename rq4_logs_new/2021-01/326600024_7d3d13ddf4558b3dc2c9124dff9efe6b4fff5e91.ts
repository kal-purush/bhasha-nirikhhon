import { PromoEnrollmentRequest as PromoEnrollmentRequestModel } from '../../../lib/mongoose/models/promo-enrollment-request';
import { PromoEnrollmentRequestsStore } from '../../../types/index';

const actions = ({
  PromoEnrollmentRequest,
}: {
  PromoEnrollmentRequest: typeof PromoEnrollmentRequestModel;
}): PromoEnrollmentRequestsStore => {
  async function insertPromoEnrollment(info) {
    return PromoEnrollmentRequest.create(info);
  }

  async function promoEnrollmentExistsByFilter(filters) {
    return PromoEnrollmentRequest.exists(filters);
  }

  async function selectOnePromoEnrollmentByFilters(filters) {
    return PromoEnrollmentRequest.findOne(filters)
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