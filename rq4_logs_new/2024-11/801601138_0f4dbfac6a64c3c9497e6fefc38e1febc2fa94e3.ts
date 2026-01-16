import { ApiSettings, getApiDomainHeaders } from "@/shared/api";
import { TwinStatusCreateRq, TwinStatusUpdateRq } from "./types";

export function createTwinStatusApi(settings: ApiSettings) {
  function create({
    twinClassId,
    data,
  }: {
    twinClassId: string;
    data: TwinStatusCreateRq;
  }) {
    return settings.client.POST(
      "/private/twin_class/{twinClassId}/twin_status/v1",
      {
        params: {
          header: getApiDomainHeaders(settings),
          path: { twinClassId: twinClassId },
        },
        body: data,
      }
    );
  }

  function update({
    statusId,
    data,
  }: {
    statusId: string;
    data: TwinStatusUpdateRq;
  }) {
    return settings.client.PUT("/private/twin_status/{twinStatusId}/v1", {
      params: {
        header: getApiDomainHeaders(settings),
        path: { twinStatusId: statusId },
      },
      body: data,
    });
  }

  function getById({ twinStatusId }: { twinStatusId: string }) {
    return settings.client.GET(`/private/twin_status/{twinStatusId}/v1`, {
      params: {
        header: getApiDomainHeaders(settings),
        path: { twinStatusId },
        query: {
          showStatusMode: "DETAILED",
        },
      },
    });
  }

  // NOTE: Emulating `/private/twin_status/search/v1`
  // TODO: replace as per ${jira-task-link}
  function search({
    twinClassId,
    search,
  }: {
    twinClassId?: string;
    search?: string;
  }) {
    return settings.client
      .POST("/private/twin_class/search/v1", {
        params: {
          header: getApiDomainHeaders(settings),
          query: {
            lazyRelation: false,
            showTwinClassMode: "SHORT",
            showTwinClass2StatusMode: "DETAILED",
            limit: 10,
            offset: 0,
          },
        },
        body: {
          twinClassIdList: twinClassId ? [twinClassId] : [],
        },
      })
      .then((response) => {
        if (response.data?.relatedObjects?.statusMap) {
          const lowerSearch = search?.toLowerCase() || "";
          const record = response.data.relatedObjects.statusMap;

          const newRecord = Object.entries(record).reduce(
            (acc, [key, value]) => {
              if (
                value.key?.toLowerCase().includes(lowerSearch) ||
                value.name?.toLowerCase().includes(lowerSearch)
              ) {
                acc[key] = value;
              }
              return acc;
            },
            {} as { [key: string]: any }
          );

          response.data.relatedObjects.statusMap = newRecord;
        }

        return response;
      });
  }

  return { create, update, getById, search };
}

export type TwinStatusApi = ReturnType<typeof createTwinStatusApi>;