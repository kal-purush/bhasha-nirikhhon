import { AutoFormValueInfo, AutoFormValueType } from "@/components/auto-field";
import {
  toArray,
  toArrayOfString,
  wrapWithPercent,
  type FilterFeature,
} from "@/shared/libs";
import { z } from "zod";
import { PermissionFilterKeys, PermissionFilters } from "../../api";

export function usePermissionFilters(): FilterFeature<
  PermissionFilterKeys,
  PermissionFilters
> {
  function buildFilterFields(): Record<
    PermissionFilterKeys,
    AutoFormValueInfo
  > {
    return {
      idList: {
        type: AutoFormValueType.tag,
        label: "Id",
        schema: z.string().uuid("Please enter a valid UUID"),
        placeholder: "Enter UUID",
      },
      keyLikeList: {
        type: AutoFormValueType.tag,
        label: "Key",
      },
      nameLikeList: {
        type: AutoFormValueType.tag,
        label: "Name",
      },
      descriptionLikeList: {
        type: AutoFormValueType.tag,
        label: "Description",
      },
    } as const;
  }

  function mapFiltersToPayload(
    filters: Record<PermissionFilterKeys, unknown>
  ): PermissionFilters {
    const result: PermissionFilters = {
      idList: toArrayOfString(filters.idList),
      keyLikeList: toArrayOfString(filters.keyLikeList).map(wrapWithPercent),
      nameLikeList: toArrayOfString(filters.nameLikeList).map(wrapWithPercent),
      descriptionLikeList: toArrayOfString(
        toArray(filters.descriptionLikeList)
      ).map(wrapWithPercent),
    };

    return result;
  }

  return {
    buildFilterFields,
    mapFiltersToPayload,
  };
}