import { AutoFormValueInfo, AutoFormValueType } from "@/components/auto-field";
import {
  TwinClass_DETAILED,
  useTwinClassSelectAdapter,
} from "@/entities/twinClass";
import {
  toArray,
  toArrayOfString,
  wrapWithPercent,
  type FilterFeature,
} from "@/shared/libs";
import { TwinFilterKeys, TwinFilters } from "../../api";

export function useTwinFilters(): FilterFeature<TwinFilterKeys, TwinFilters> {
  const adapter = useTwinClassSelectAdapter();

  function buildFilterFields(): Record<TwinFilterKeys, AutoFormValueInfo> {
    return {
      twinIdList: {
        type: AutoFormValueType.uuid,
        label: "ID",
      },
      twinClassIdList: {
        type: AutoFormValueType.multiCombobox,
        label: "Twin Class",
        getById: adapter.getById,
        getItems: adapter.getItems,
        getItemKey: (item: unknown) =>
          adapter.getItemKey(item as TwinClass_DETAILED),
        getItemLabel: (item: unknown) =>
          adapter.getItemLabel(item as TwinClass_DETAILED),
        multi: true,
      },
      statusIdList: {
        // TODO: replace with combobox
        // in TWINFACES-117: https://alcosi.atlassian.net/browse/TWINFACES-117
        type: AutoFormValueType.string,
        label: "Status ID",
      },
      twinNameLikeList: {
        // TODO: replace with combobox
        // via using useTwinSelectAdapter
        type: AutoFormValueType.string,
        label: "Name",
      },
      createdByUserIdList: {
        type: AutoFormValueType.string,
        label: "Author ID",
      },
      assignerUserIdList: {
        type: AutoFormValueType.string,
        label: "Assigner ID",
      },
      headTwinIdList: {
        type: AutoFormValueType.multiCombobox,
        label: "Head",
        getById: adapter.getById,
        getItems: adapter.getItems,
        getItemKey: (item: unknown) =>
          adapter.getItemKey(item as TwinClass_DETAILED),
        getItemLabel: (item: unknown) =>
          adapter.getItemLabel(item as TwinClass_DETAILED),
        multi: true,
      },
      tagDataListOptionIdList: {
        type: AutoFormValueType.string,
        label: "Tags",
      },
      markerDataListOptionIdList: {
        type: AutoFormValueType.string,
        label: "Markers",
      },
    } as const;
  }

  function mapFiltersToPayload(
    filters: Record<TwinFilterKeys, unknown>
  ): TwinFilters {
    const result: TwinFilters = {
      twinIdList: toArrayOfString(toArray(filters.twinIdList), "id"),
      twinNameLikeList: toArrayOfString(
        toArray(filters.twinNameLikeList),
        "name"
      ).map(wrapWithPercent),
      twinClassIdList: toArrayOfString(toArray(filters.twinClassIdList), "id"),
      statusIdList: toArrayOfString(toArray(filters.statusIdList), "id"),
      createdByUserIdList: toArrayOfString(
        toArray(filters.createdByUserIdList),
        "id"
      ),
      assignerUserIdList: toArrayOfString(
        toArray(filters.assignerUserIdList),
        "id"
      ),
      headTwinIdList: toArrayOfString(toArray(filters.headTwinIdList), "id"),
      tagDataListOptionIdList: toArrayOfString(
        toArray(filters.tagDataListOptionIdList)
      ),
      markerDataListOptionIdList: toArrayOfString(
        toArray(filters.markerDataListOptionIdList)
      ),
    };

    return result;
  }

  return {
    buildFilterFields,
    mapFiltersToPayload,
  };
}