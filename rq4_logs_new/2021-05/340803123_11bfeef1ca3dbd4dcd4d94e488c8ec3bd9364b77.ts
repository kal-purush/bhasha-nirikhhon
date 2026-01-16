import { useRoute, useRouter } from "vue-router";
import { useOrgEventApi } from "./api";
import { useResult } from "@vue/apollo-composable";

const useOrgEventHooks = () => {
  const route = useRoute();
  const router = useRouter();
  const eventId = Number(route.params.id);
  const { result, onError, refetch } = useOrgEventApi({
    id: eventId
  });

  onError(() => {
    router.push("/404");
  });

  const event = useResult(result, null, data => data.event);
  return { event, eventId, refetch };
};

export default useOrgEventHooks;