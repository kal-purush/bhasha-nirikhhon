import { TFunction } from "i18next";

export function generateSelectorObject(
    filterAttribute: string,
    selectorArray: string[],
    t?: TFunction,
): { value: string; label: string }[] {
    return selectorArray.map((filterValue) => {
        if (t !== undefined) {
            const filterValueLabel = t(
                `FilterValues.${filterAttribute}.${filterValue.replace(
                    ".",
                    ""
                )}`
            );
            return { value: filterValue, label: filterValueLabel };
        }
        return { value: filterValue, label: filterValue };
    });
}