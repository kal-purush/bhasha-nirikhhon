import { Employee } from "../../../../redux/features/hr/hrTypes";
import { EmployeeDataInterface, FileData } from "./EmployeeDataInterfaces";
import { formatDate, calculateRemainingDays } from "../../utils/utils";

export const transformEmployeeToEmployeeData = (
  employee: Employee
): EmployeeDataInterface => {
  const remainingDays = calculateRemainingDays(
    employee.visaStatus.startDate,
    employee.visaStatus.endDate
  );
  const nextStep = determineNextStep(employee);

  return {
    employee_id: employee._id,
    name: `${employee.personalInformation.firstName} ${employee.personalInformation.lastName}`,
    work_auth: employee.personalInformation.workAuth,
    start_day: formatDate(employee.visaStatus.startDate),
    end_day: formatDate(employee.visaStatus.endDate),
    remaining: remainingDays,
    next_step: nextStep,
  };
};

const determineNextStep = (employee: Employee): string => {
  if (!employee.isActive) {
    return "Send Registration Token";
  }

  if (employee.personalInformation.workAuth !== "F1(CPT/OPT)") {
    return "Completed";
  }

  const requiredDocs = ["OPT Receipt", "OPT EAD", "I-983", "I-20"];

  for (const docType of requiredDocs) {
    const doc = employee.visaStatus.documents.find((d) => d.type === docType);
    if (!doc || doc.status === "Rejected") {
      return `Submit ${docType}`;
    }
    if (doc.status === "Pending") {
      return "Wait for Approval";
    }
  }
  return "Completed";
};

export const transformVisaDocuments = (employee: Employee): FileData[] => {
  return employee.visaStatus.documents
    .map((doc) => {
      return {
        name: doc.type,
        modifiedDate: formatDate(new Date().toString()),
        status: doc.status,
        canDownload: true,
        canPreview: true,
        documentUrl: doc.url,
      };
    });
};