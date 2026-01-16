export interface TableSettings {
	columns: ColumnSettings[];
	mobile: string[];

	// default sort
	active: string; // property
	dir: ColumnDir;

	trackBy: (index: number, item: any) => string;

}

export interface ColumnSettings {
	header: string;

	property: string;

	type?: ColumnType; // Defaults to Normal
	displayFormat?: (property: any, obj?: object, all?: object[]) => string;

	func?: (input: any, obj?: object, all?: object[]) => void;
	disabled?: (input: any, obj?: object, all?: object[]) => boolean;
	icon?: (property: string) => string;
	noText?: boolean;
	narrow?: boolean;

	// classes
	sort?: boolean;
	rightAlign?: boolean;
}

export enum ColumnType {
	Button,
	InternalLink,
	ExternalLink,
	Image,
	Normal
}

export enum ColumnDir {
	ASC = 'asc',
	DESC = 'desc'
}
