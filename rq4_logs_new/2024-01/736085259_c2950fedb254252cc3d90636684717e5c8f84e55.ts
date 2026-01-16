import { Employee, Document } from "../../../../redux/features/hr/hrTypes";
import { formatDate } from "../../utils/utils";
import {
  EmployeeDataInterface,
  DocumentInterface,
} from "../EmployeeDataInterfaces";

export function transformEmployeeToProfileData(
  employee: Employee
): EmployeeDataInterface {
  return {
    avatar: !employee.personalInformation.profilePicture
      ? "https://wallpapers-clan.com/wp-content/uploads/2023/06/sad-ghost-dark-blue-background.jpg"
      : employee.personalInformation.profilePicture,
    basic_info: {
      name: `${employee.personalInformation.firstName} ${employee.personalInformation.lastName}`,
      employee_id: employee.personalInformation._id.toString(),
      ssn: employee.personalInformation.ssn,
      work_auth: employee.personalInformation.workAuth,
      phone: employee.personalInformation.phoneNumbers.cell,
      email: employee.email,
      dob: formatDate(employee.personalInformation.dateOfBirth),
      gender: employee.personalInformation.gender,
      preferred_name: employee.personalInformation.preferredName || "",
    },
    address: {
      street: employee.personalInformation.address.street,
      building: employee.personalInformation.address.building,
      city: employee.personalInformation.address.city,
      state: employee.personalInformation.address.state,
      zip_code: employee.personalInformation.address.zip,
    },
    contact: {
      email: employee.email,
      cell_phone: employee.personalInformation.phoneNumbers.cell,
      work_phone: employee.personalInformation.phoneNumbers.work || "",
    },
    visa_status: {
      visa_title: employee.visaStatus.visaType,
      start_date: formatDate(employee.visaStatus.startDate),
      end_date: formatDate(employee.visaStatus.endDate),
    },
    reference: {
      name: `${employee.personalInformation.reference.firstName} ${employee.personalInformation.reference.lastName}`,
      phone_number: employee.personalInformation.reference.phone,
      email: employee.personalInformation.reference.email,
      relationship: employee.personalInformation.reference.relationship,
    },
    emergency_contact1: {
      name: `${employee.personalInformation.emergencyContacts[0].firstName} ${employee.personalInformation.emergencyContacts[0].lastName}`,
      phone: employee.personalInformation.emergencyContacts[0].phone,
      relationship:
        employee.personalInformation.emergencyContacts[0].relationship,
      email: employee.personalInformation.emergencyContacts[0].email,
    },
    emergency_contact2: {
      name: `${employee.personalInformation.emergencyContacts[1].firstName} ${employee.personalInformation.emergencyContacts[1].lastName}`,
      phone: employee.personalInformation.emergencyContacts[1].phone,
      relationship:
        employee.personalInformation.emergencyContacts[1].relationship,
      email: employee.personalInformation.emergencyContacts[1].email,
    },
  };
}

export function transformProfileDocuments (documents: Document[]): DocumentInterface[] {
  return documents.map((doc) => ({
    documentName: doc.type,
    lastModifiedDate: "",
    documentSize: "",
    canDownload: true,
    canPreview: true,
    documentUrl: doc.url,
  }));
};