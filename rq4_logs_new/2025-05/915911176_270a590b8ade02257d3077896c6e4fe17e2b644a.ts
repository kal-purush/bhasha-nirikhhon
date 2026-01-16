//*Shifts
export interface IShiftsColumnsType {
	uuid: string;
	id: string;
	name: string;
	startTime: string;
	endTime: string;
	late_time: string;
	early_exit_before: string;
	first_half_end: string;
	break_time_end: string;
	default_shift: boolean;
	color: string;
	status: boolean;
}
//*Shifts Group
export interface IShiftsGroupColumnsType {
	uuid: string;
	name: string;
	id: string;
	default_shift: boolean;
	status: boolean;
	off_days: string[];
	shift_uuid: string;
	shift_name: string;
	effective_date: string;
}
//* Roaster
export interface IRoasterColumnsType {
	uuid: string;
	shift_uuid: string;
	shift_group_uuid: string;
	shift_name: string;
	shift_group_name: string;
}