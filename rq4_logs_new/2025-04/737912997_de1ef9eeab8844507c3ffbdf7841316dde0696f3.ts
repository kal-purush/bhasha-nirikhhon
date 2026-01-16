import { MeterDataWithObjectId } from "@/store/models/meter-data";
import { getTableMeterDataColumns } from "@/components/features/meters-data/table-config/table-columns";
import { getTableMeterDataRows } from "@/components/features/meters-data/table-config/table-rows";
import { TableConfig } from "@/components/shared/table/table-config";
import {
    getTableUtilityPriceColumns,
    getTableUtilityPriceRows,
} from "@/components/shared/table/constants/table-utility-price";
import { UtilityPrice } from "@/store/models/utility-price";
import { iconNames } from "@/components/ui/icon/icon-constants";
import { TableAction } from "@/components/shared/table/table-models";
import { AppDispatch } from "@/store/store";

export function initialTableMeterDataConfig(
    data: MeterDataWithObjectId[],
    isVisibleWaterColumn: boolean,
    dispatch: AppDispatch
) {
    const tableConfig: TableConfig = {
        columns: getTableMeterDataColumns(isVisibleWaterColumn),
        rows: getTableMeterDataRows(data, isVisibleWaterColumn, dispatch),
        action: createAddRowAction(),
    };

    return tableConfig;
}

export function initialUtilityPriceTableConfig(data: UtilityPrice[]) {
    const tableConfig: TableConfig = {
        columns: getTableUtilityPriceColumns(),
        rows: getTableUtilityPriceRows(data),
        action: createAddRowAction(),
    };

    return tableConfig;
}

export function createAddRowAction(onClick = () => {}, visible = true, label = "Додати") {
    const tableAction: TableAction = {
        icon: iconNames.plusCircle,
        onClick,
        visible,
        label,
    };

    return tableAction;
}