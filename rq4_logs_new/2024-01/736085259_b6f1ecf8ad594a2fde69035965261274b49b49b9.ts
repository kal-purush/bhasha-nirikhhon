import { Employee } from "../../../../redux/features/hr/hrTypes";
import { RegDataInterface } from "./EmployeeDataInterfaces";

export const transformEmployeeToRegData = (
  employee: Employee
): RegDataInterface => {
  const employeeId = employee._id;
  const name = `${employee.personalInformation.firstName} ${employee.personalInformation.lastName}`;
  const email = employee.email;
  const tokenExpired =
    employee.registrationToken &&
    new Date(employee.registrationToken.expiry) < new Date();
  const generateTokenAndSendEmail =
    !employee.isActive && (!employee.registrationToken || tokenExpired);
  const status = employee.isActive ? "Active" : "Inactive";

  return {
    employee_id: employeeId,
    name,
    email,
    generate_token_and_send_email: generateTokenAndSendEmail,
    status,
  };
};