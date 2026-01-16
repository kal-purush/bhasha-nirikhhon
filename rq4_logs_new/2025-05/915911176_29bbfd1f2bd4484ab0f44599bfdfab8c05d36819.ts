import { AddressData } from '../address-information';
import { EmployeeData } from '../general-information';

export const approverData = {
	firstLeaveApprover: 'Israk Ahmed Shourav',
	secondLeaveApprover: 'Not Assigned',
	firstLateApprover: 'Israk Ahmed Shourav',
	secondLateApprover: 'Not Assigned',
	firstManualEntryApprover: 'Israk Ahmed Shourav',
	secondManualEntryApprover: 'Not Assigned',
};

export const employeeData: EmployeeData = {
	name: 'Rashik Buksh Rafsan',
	id: 'FZL-2453',
	email: 'rafsan@fortunetip.com',
	department: 'IT',
	subDepartment: 'Not Set',
	designation: 'Engineer',
	employmentType: 'Permanent',
	shiftGroup: 'Regular',
	gender: 'Male',
	primaryText: 'Arigato',
	secondaryText: 'Rafsan',
	rfid: '0012274423',
	reportPosition: '1',
	leavePolicy: 'Policy 2',
	joiningDate: '01/10/2023',
	endDate: 'Not Set',
	workplace: 'Head Office',
	lineManager: 'Not Assigned',
	hrManager: 'Israk Ahmed Shourav',
};

export const personalContactData = {
	fatherName: 'Anayet Buksh',
	motherName: 'Runa Layla',
	bloodGroup: 'AB+',
	dateOfBirth: '08/12/2000',
	nationalId: '512016265',
	officePhone: 'Not Set',
	homePhone: 'Not Set',
	personalPhone: '01684545111',
};

export const addressData: AddressData[] = [
	{
		type: 'Permanent',
		address: 'National ideal School, Khilgaon',
		thana: 'Khilgaon',
		district: 'Dhaka',
	},
];

export const employmentData: any[] = [];

export const educationData = [
	{
		id: '1',
		degree: 'BSC',
		institution: 'East West University',
		board: 'Dep. Computer Science & Engineering',
		year: '2023',
		grade: '3.20',
	},
];

export const documentsData = [
	{
		id: '1',
		type: 'NID',
		description: 'National ID Card',
		link: '#',
	},
];