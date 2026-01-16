//* Department
export type IDepartmentTableData = {
	uuid: string;
	department: string;
	created_at: string;
	updated_at: string;
	remarks: string;
};

//* Designation
export type IDesignationTableData = {
	uuid: string;
	designation: string;
	created_at: string;
	updated_at: string;
	remarks: string;
};

//* User
export type IUserTableData = {
	uuid: string;
	name: string;
	email: string;
	business_type: 'company' | 'individual';
	designation_uuid: string;
	designation: string;
	department_uuid: string;
	department: string;
	user_type: 'employee' | 'customer';
	ext: string;
	phone: string;
	created_at: string;
	updated_at: any;
	status: string;
	remarks: string;
};

//* Reset Password
export type IResetPassword = {
	uuid: string;
	name: string;
};

//* Page Assign
export type IPageAssign = {
	uuid: string;
	name: string;
};

//* Employee
export type IEmployeeTableData = {
	uuid: string;
	id: number;
	gender: string;
	user_uuid: string;
	users_name: any;
	start_date: string;
	workplace_uuid: string;
	workplace_name: string;
	rfid: string;
	sub_department_uuid: string;
	sub_department_name: string;
	primary_display_text: string;
	secondary_display_text: string;
	configuration_uuid: string;
	employment_type_uuid: string;
	end_date: string;
	shift_group_uuid: string;
	shift_group_name: string;
	line_manager_uuid: string;
	hr_manager_uuid: string;
	is_admin: boolean;
	is_hr: boolean;
	is_line_manager: boolean;
	allow_over_time: boolean;
	exclude_from_attendance: boolean;
	status: boolean;
	created_by: string;
	created_by_name: string;
	created_at: string;
	updated_at: string;
	remarks: string;
	name: string;
	email: string;
	pass: string;
	designation_uuid: string;
	designation_name: string;
	department_uuid: string;
	department_name: string;
	employee_id: string;
	leave_policy_uuid: string;
	leave_policy_name: string;
	report_position: string;
};