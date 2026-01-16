import { AxiosRequestConfig } from "axios";
import { RecordedShow } from "../domain/RecordedShow";
import { httpClient } from "../services/http.client";

export interface UploadRecordedShowParms {
  programId: string | number;
  recordedShow: any;
}
export const uploadRecordedShow = async (
  args: UploadRecordedShowParms,
  opts?: AxiosRequestConfig
) => {
  const form = new FormData();
  form.append("recordedShow", args.recordedShow);

  const response = await httpClient.post(
    `/v2/programs/${args.programId}/recordedShows`,
    form,
    {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      ...opts,
    }
  );

  return response.data;
};

export const publishRecordedShow = async (args: {
  recordedShowId: number | string;
  programId: number | string;
  name: string;
}) => {
  const { data } = await httpClient.post<{ recordedShow: RecordedShow }>(
    `/v2/programs/${args.programId}/recordedShows/${args.recordedShowId}/publish`,
    {
      name: args.name,
    }
  );

  return data;
};

export const getRecordedShow = async (id: string | number) => {
  const { data } = await httpClient.get<{ recordedShow: RecordedShow }>(
    `/v2/recorded-shows/${id}`
  );
  return data;
};