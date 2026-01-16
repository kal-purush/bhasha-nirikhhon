import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "react-toastify";
import { post } from "..";
import { PanelSettings } from "../../../types";
import { Paths, useGet } from "../factory";

const baseUrl = `${Paths.PanelControl}/panel-settings`;
export function createPanelSettings(
  settings: Partial<PanelSettings>
): Promise<PanelSettings> {
  return post<Partial<PanelSettings>, PanelSettings>({
    path: baseUrl,
    payload: settings,
  });
}

export function useCreateMultiplePageMutation() {
  const queryKey = [baseUrl];
  const queryClient = useQueryClient();
  return useMutation(createPanelSettings, {
    onMutate: async () => {
      await queryClient.cancelQueries(queryKey);
    },

    onError: (_err: any) => {
      const errorMessage =
        _err?.response?.data?.message || "An unexpected error occurred";
      setTimeout(() => toast.error(errorMessage), 200);
    },
  });
}

export function useGetPanelSettings() {
  return useGet<PanelSettings>(baseUrl);
}