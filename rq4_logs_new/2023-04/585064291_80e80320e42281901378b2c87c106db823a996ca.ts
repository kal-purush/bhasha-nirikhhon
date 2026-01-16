import axios from "axios";

export const deleteSubs = async ({ subscribeId }: { subscribeId: number }) => {
  const queryKey = `/api/v1/subscribe/${subscribeId}`;
  const response = await axios.delete(queryKey);
  return response.data;
};