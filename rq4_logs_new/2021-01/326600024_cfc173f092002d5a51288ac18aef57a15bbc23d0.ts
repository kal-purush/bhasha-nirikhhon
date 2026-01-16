import {
  PromoEnrollmentRequestsStore,
  PromoEnrollmentRequestDocument,
  UseCase,
} from '../../types';

const selectAllPromoEnrollmentRequests = ({
  promoEnrollmentRequestsStore,
}: {
  promoEnrollmentRequestsStore: PromoEnrollmentRequestsStore;
}): UseCase<PromoEnrollmentRequestDocument[]> => {
  return async function useCase() {
    const promoEnrollmentRequests = await promoEnrollmentRequestsStore.selectAllPromoEnrollmentRequests();
    return promoEnrollmentRequests;
  };
};

export default selectAllPromoEnrollmentRequests;