import { Option } from "@/components/ui/input-group/input-group-models";
import { tableMeterDataColumnKeys } from "@/components/features/meters-data/table-config/table-columns/table-columns-enums";

export const tableMeterDataColumnIdOption: Option = {
    label: "ID",
    value: tableMeterDataColumnKeys.id,
};

export const tableMeterDataColumnCreatedAtOption: Option = {
    label: "Створено",
    value: tableMeterDataColumnKeys.createdAt,
};

export const tableMeterDataColumnUpdatedAtOption: Option = {
    label: "Оновлено",
    value: tableMeterDataColumnKeys.updatedAt,
};

export const tableMeterDataColumnVisibilityOptions: Option[] = [
    tableMeterDataColumnIdOption,
    tableMeterDataColumnCreatedAtOption,
    tableMeterDataColumnUpdatedAtOption,
];