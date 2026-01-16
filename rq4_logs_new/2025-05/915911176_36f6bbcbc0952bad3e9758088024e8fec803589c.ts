//* Problems Columns
export type IProblemsTableData = {
	uuid: string;
	name: string;
	category: 'employee' | 'customer';
	remarks: string;
	created_by: string;
	created_at: string;
	updated_at: string;
};
//* Diagnosis Columns
export type IDiagnosisTableData = {
	uuid: string;
	id: string;
	diagnosis_id: string;
	order_uuid: string;
	info_uuid: string;
	order_id: string;
	engineer_uuid: string;
	problems_uuid: string[];
	problems_name: string[];

	problem_statement: string;
	customer_problem_statement: string;
	status: 'pending' | 'rejected' | 'accepted' | 'not_repairable';
	status_update_date: string;
	proposed_cost: number;
	is_proceed_to_repair: boolean;
	customer_remarks: string;
	remarks: string;
	created_by: string;
	created_at: string;
	updated_at: string;
};
//* Order Columns
export type IOrderTableData = {
	id: string;
	order_id: string;
	is_proceed_to_repair: boolean;
	uuid: string;
	user_name: string;
	user_phone: string;
	is_product_received: boolean;
	is_diagnosis_needed: boolean;
	is_delivery_complete?: boolean;
	received_date: string;
	user_id: string;
	model_uuid: string;
	model_name: string;
	brand_name: string;
	size_uuid: string;
	size_name: string;
	serial_no: string;
	problems_uuid: string[];
	problems_name: string[];
	problem_statement: string;
	accessories: string[];
	accessories_name: string[];
	info_uuid: string;
	info_id: string;
	is_transferred_for_qc: boolean;
	is_ready_for_delivery: boolean;
	is_diagnosis_need: boolean;
	branch_name: string;
	warehouse_uuid: string;
	warehouse_name: string;
	rack_uuid: string;
	rack_name: string;
	floor_uuid: string;
	floor_name: string;
	box_uuid: string;
	box_name: string;
	quantity: number;
	created_by: string;
	created_at: string;
	updated_at: string;
	diagnosis?: IDiagnosisTableData;
	process?: IProcessTableData[];
	remarks: string;
};
//* Info Columns
export type IInfoTableData = {
	uuid: string;
	is_new_customer?: boolean;
	name: string;
	phone: string;
	user_phone: string;
	user_id: string;
	id: string;
	info_id: string;
	user_uuid: string;
	user_name: string;
	zone_name: string;
	location: string;
	designation_uuid: string;
	department_uuid: string;
	remarks: string;
	created_by: string;
	created_at: string;
	updated_at: string;
	is_product_received: boolean;
	received_date: string;
	order_entry: IOrderTableData[];
};

//* Section Columns
export type ISectionTableData = {
	uuid: string;
	name: string;
	remarks: string;
	created_by: string;
	created_at: string;
	updated_at: string;
};
//* Process Columns
export type IProcessTableData = {
	id: string;
	index: number;
	process_id: string;
	uuid: string;
	section_uuid: string;
	section_name: string;
	order_uuid: string;
	diagnosis_uuid: string;
	engineer_uuid: string;
	problems_uuid: string[];
	problems_name: string[];
	problem_statement: string;
	status: boolean;
	status_update_date: string;
	is_transferred_for_qc: boolean;
	is_ready_for_delivery: boolean;
	warehouse_uuid: string;
	rack_uuid: string;
	floor_uuid: string;
	box_uuid: string;
	process_uuid: string;
	remarks: string;
	created_by: string;
	created_at: string;
	updated_at: string;
};

//*Zone
export type IZoneTableData = {
	uuid: string;
	id: number;
	division: string;
	latitude: string;
	longitude: string;
	name: string;
	remarks: string;
	created_by: string;
	created_at: string;
	updated_at: string;
};

//* Accessories
export type IAccessoriesTableData = {
	uuid: string;
	name: string;
};