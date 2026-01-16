//* Department
export type IDepartmentTableData = {
	uuid: string;
	department: string;
	hierarchy: number;
	status: boolean;
	created_at: string;
	updated_at: string;
	remarks: string;
};

//* Designation
export type IDesignationTableData = {
	uuid: string;
	designation: string;
	hierarchy: number;
	created_at: string;
	updated_at: string;
	remarks: string;
};

//* Employee Type
export type IEmployeeTypeTableData = {
	uuid: string;
	name: string;
	status: boolean;
	id: number;
	created_at: string;
	updated_at: string;
	remarks: string;
};
//*holidays
export type IHolidayTableData = {
	uuid: string;
	id: number;
	name: string;
	date: string | Date;
	created_at: string;
	updated_at: string;
	remarks: string;
};
//* Specials Days
export type ISpecialDaysTableData = {
	uuid: string;
	id: number;
	name: string;
	from_date: string | Date;
	to_date: string | Date;
	workplace_uuid: string;
	created_at: string;
	updated_at: string;
	remarks: string;
};
//* Sub Departments
export type ISubDepartmentTableData = {
	uuid: string;
	name: string;
	hierarchy: number;
	status: boolean;
	created_at: string;
	updated_at: string;
	remarks: string;
};

//* Workplace
export type IWorkplaceTableData = {
	uuid: string;
	name: string;
	hierarchy: number;
	status: boolean;
	created_at: string;
	updated_at: string;
	remarks: string;
	latitude: number;
	longitude: number;
};