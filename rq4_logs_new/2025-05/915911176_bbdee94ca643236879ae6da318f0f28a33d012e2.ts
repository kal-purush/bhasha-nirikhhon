interface ITableFilterOptionSSRDefault<T> {
	accessor: keyof T;
	label: string;
	isPinned?: boolean;
}

export type ITableFilterOptionSSRSelectStatic = {
	type: 'select';
	mode: 'static';
	options: IFormSelectOption[];
};

export type ITableFilterOptionSSRSelectDynamic = {
	type: 'select';
	mode: 'dynamic';
	apiUrl: string;
};

export type ITableFilterOptionSSRSelect = ITableFilterOptionSSRSelectStatic | ITableFilterOptionSSRSelectDynamic;

type ITableFilterOptionSSROthers = {
	type: 'checkbox' | 'radio' | 'date-range' | 'date' | 'text';
};

// type: 'select' | 'checkbox' | 'radio' | 'date-range' | 'date' | 'text';
export type ITableFilterOptionSSR<T> = ITableFilterOptionSSRDefault<T> &
	(ITableFilterOptionSSRSelect | ITableFilterOptionSSROthers);