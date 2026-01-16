import { useCurrentUser } from "./useCurrentUser";
import { SeparateApprovalStatusAttributes } from "$interfaces/Secretary";

export const useAdminApprovalStatusAttribute = () => {
  const response = useCurrentUser();
  const admin = response.data?.getCurrentUser?.admin;
  const approvalStatusAttribute = admin?.isGraduados() ?
    SeparateApprovalStatusAttributes.graduados : SeparateApprovalStatusAttributes.extension;

  return {
    ...response,
    data: {
      approvalStatusAttribute
    }
  };
};