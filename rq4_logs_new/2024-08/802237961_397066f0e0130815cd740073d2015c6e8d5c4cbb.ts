import { useActivateFranchise } from "./activateFranchise";
import { useListFranchiseAgreements } from "./agreements/listAgreements";
import { useUpdateAgreements } from "./agreements/updateAgreements";
import { useCreateFranchise } from "./createFranchise";
import { useGetFranchiseById } from "./getFranchiseById";
import { useInactivateFranchise } from "./inactivateFranchise";
import { useListFranchises } from "./listFranchises";
import { useUpdateFranchise } from "./updateFranchise";
import { useValidateStepOne } from "./validation/validateStepOne";
import { useValidateStepTwo } from "./validation/validateStepTwo";

export const FranchiseServices = {
  list: useListFranchises,
  getById: useGetFranchiseById,
  disable: useInactivateFranchise,
  enable: useActivateFranchise,
  create: useCreateFranchise,
  update: useUpdateFranchise,
  validateStepOne: useValidateStepOne,
  validateStepTwo: useValidateStepTwo,
  listAgreement: useListFranchiseAgreements,
  updateAgrement: useUpdateAgreements,
};