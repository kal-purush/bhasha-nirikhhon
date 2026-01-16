import { AutoFormValueInfo, AutoFormValueType } from "@/components/auto-field";
import {
  type FilterFeature,
  toArray,
  toArrayOfString,
  wrapWithPercent,
} from "@/shared/libs";
import { DatalistApiFilters, DatalistFilterFields } from "@/entities/datalist";

export function useDatalistFilters(): FilterFeature<
  DatalistFilterFields,
  DatalistApiFilters
> {
  function buildFilterFields(): Record<
    DatalistFilterFields,
    AutoFormValueInfo
  > {
    return {
      idList: {
        type: AutoFormValueType.uuid,
        label: "Id",
      },
      nameLikeList: {
        type: AutoFormValueType.string,
        label: "Name",
      },
      descriptionLikeList: {
        type: AutoFormValueType.string,
        label: "Description",
      },
      keyLikeList: {
        type: AutoFormValueType.string,
        label: "Key",
      },
    };
  }

  function mapFiltersToPayload(
    filters: Record<DatalistFilterFields, unknown>
  ): DatalistApiFilters {
    const result: DatalistApiFilters = {
      idList: toArrayOfString(toArray(filters.idList), "id"),
      nameLikeList: toArrayOfString(toArray(filters.nameLikeList), "name").map(
        wrapWithPercent
      ),
      descriptionLikeList: toArrayOfString(
        toArray(filters.descriptionLikeList),
        "description"
      ).map(wrapWithPercent),
      keyLikeList: toArrayOfString(toArray(filters.keyLikeList), "key").map(
        wrapWithPercent
      ),
    };
    return result;
  }

  return {
    buildFilterFields,
    mapFiltersToPayload,
  };
}