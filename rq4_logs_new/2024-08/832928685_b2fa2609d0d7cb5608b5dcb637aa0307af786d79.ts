import { ITableMeta, ITable } from '../interface/table';
import TableService from '../service/table-service';

export default class TableController {
	private tableService: TableService;

	constructor() {
		this.tableService = new TableService();
	}

	public create = async (inputTable: Partial<ITable>): Promise<string> => {
		// Pass the validated data to the service layer
		return (await this.tableService.create(inputTable))._id!;
	};

	public get = async (): Promise<ITableMeta> => {
		// Call the service layer to get the table
		return await this.tableService.get();
	};

	public getById = async (id: string): Promise<ITable> => {
		// Call the service layer to get the table
		return await this.tableService.getById(id);
	};

	public patch = async (id: string, updatedTableData: Partial<ITable>): Promise<void> => {
		return await this.tableService.patch(id, updatedTableData);
	};

	public activate = async (id: string): Promise<void> => {
		return await this.tableService.activate(id);
	};

	public getTableByRestaurantSlug = async (restaurantSlug: string): Promise<ITable[]> => {
		return await this.tableService.getTableByRestaurantSlug(restaurantSlug);
	};
}