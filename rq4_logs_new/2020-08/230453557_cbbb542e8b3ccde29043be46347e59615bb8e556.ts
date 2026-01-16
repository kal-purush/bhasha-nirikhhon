import { IEditableDetailValues } from "$pages/Applicant/EditableProfile/EditableDetail/interface";
import { applicantCareersInputArguments } from "./applicantCareersInputArguments";

export const updateCurrentApplicantArguments = ({ name, surname, ...values }: EditArguments) => ({
  ...values,
  user: { name, surname },
  capabilities: values.capabilities.map(capability => capability.description),
  careers: applicantCareersInputArguments(values.careers)
});

type EditArguments = Omit<IEditableDetailValues, "_form">;