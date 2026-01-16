import { useCurrentUser } from "./useCurrentUser";
import { Secretary, SeparateApprovalStatusColumns } from "$interfaces/Secretary";

export const useCurrentAdminSecretary = () => {
  const response = useCurrentUser();
  const secretary = response.data?.getCurrentUser?.admin?.secretary;
  const approvalStatusColumn = secretary === Secretary.graduados ?
    SeparateApprovalStatusColumns.graduados : SeparateApprovalStatusColumns.extension;

  return {
    ...response,
    data: {
      secretary,
      approvalStatusColumn
    }
  };
};

export interface IUseCurrentAdminSecretary {
  secretary: Secretary;
  approvalStatusColumn: SeparateApprovalStatusColumns;
}