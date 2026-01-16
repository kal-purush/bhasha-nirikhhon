import { useMediaQuery } from "@inubekit/inubekit";
import { addPayrollAgreementSteps } from "@config/payrollAgreement/payrollAgreementTab/assisted/steps";
import { IUseVerification } from "@ptypes/payrollAgreement/payrollAgreementTab/forms/IUseVerification";

const useVerification = (props: IUseVerification) => {
  const {
    updatedData,
    showRequestProcessModal,
    savePayrollAgreement,
    typeRegularPayroll,
    showPendingReqModal,
  } = props;

  const isTablet = useMediaQuery("(max-width: 1224px)");

  const isMobile = useMediaQuery("(max-width: 990px)");

  const canShowRequestProcess = showRequestProcessModal && savePayrollAgreement;

  const canShowPendingRequest =
    showPendingReqModal &&
    savePayrollAgreement &&
    savePayrollAgreement.requestNumber.length > 0;

  const steps = addPayrollAgreementSteps.filter((step) => {
    if (step.name.toLowerCase() === "verificación") return false;
    if (
      Number(step.id) === 4 &&
      (!updatedData.extraordinaryCycles.values ||
        updatedData.extraordinaryCycles.values.length === 0)
    ) {
      return false;
    }
    if (typeRegularPayroll) return true;
    return Number(step.id) !== 3;
  });

  return {
    steps,
    isTablet,
    isMobile,
    canShowRequestProcess,
    canShowPendingRequest,
  };
};

export { useVerification };