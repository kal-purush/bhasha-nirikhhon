import { IFormSelectOption } from '@/components/core/form/types';

export interface ICustomProductsSelectOption extends IFormSelectOption {
	
	warehouse_1: number;
	warehouse_2: number;
	warehouse_3: number;
	warehouse_4: number;
	warehouse_5: number;
	warehouse_6: number;
	warehouse_7: number;
	warehouse_8: number;
	warehouse_9: number;
	warehouse_10: number;
	warehouse_11: number;
	warehouse_12: number;

}
export interface ICustomWarehouseSelectOption extends IFormSelectOption {
	assigned: string;
}

export function getFilteredWarehouseOptions(
	productUuid: string,
	productOptions: ICustomProductsSelectOption[] = [],
	warehouseOptions: ICustomWarehouseSelectOption[] = []
) {
	const selectedProduct = productOptions.find((p) => p.value === productUuid);
	if (!selectedProduct) return [];

	const availableWarehouses = Object.keys(selectedProduct).filter(
		(key) => key.startsWith('warehouse_') && (selectedProduct as any)[key] > 0
	);

	return warehouseOptions.filter((wo) => availableWarehouses.includes(wo.assigned));
}