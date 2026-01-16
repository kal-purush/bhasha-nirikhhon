import { ExcelTables } from "../../../../types/impl/lib/impl/local";
import type { Item, Materials } from "../../../../types/impl/lib/impl/materials";
import { get as getMaterials } from "../../local/impl/get";

export const getItem = async (id: string): Promise<Item | null> => {
    const materials = (await getAll()).items;
    return Object.values(materials).find((material) => material.itemId === id) ?? null;
};

export const getAll = async (): Promise<Materials> => {
    const data = (await getMaterials(ExcelTables.ITEM_TABLE)) as Materials;
    return data;
};