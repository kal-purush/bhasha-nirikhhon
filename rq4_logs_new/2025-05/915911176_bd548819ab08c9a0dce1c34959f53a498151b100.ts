import { TLocationProps, TProductProps } from '../types';

export const ProductName = (row: TProductProps) => {
	let name = '';
	if (row.brand_name) name += row.brand_name;
	if (row.model_name) name += name ? ` - ${row.model_name}` : row.model_name;
	if (row.serial_no) name += name ? ` - ${row.serial_no}` : row.serial_no;
	return name;
};

export const LocationName = (row: TLocationProps) => {
	let name = '';
	if (row.branch_name) name += row.branch_name;
	if (row.warehouse_name) name += name ? ` - ${row.warehouse_name}` : row.warehouse_name;
	if (row.rack_name) name += name ? ` - ${row.rack_name}` : row.rack_name;
	if (row.floor_name) name += name ? ` - ${row.floor_name}` : row.floor_name;
	if (row.box_name) name += name ? ` - ${row.box_name}` : row.box_name;
	return name;
};