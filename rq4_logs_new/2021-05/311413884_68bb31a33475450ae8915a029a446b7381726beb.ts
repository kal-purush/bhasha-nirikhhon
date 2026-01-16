import axios from "axios";
import { useEffect, useState } from "react";
import { API_ENDPOINT } from "../../../constants";
import { Room } from "../../../types";

export const useGetAllRoomsOfServer = (
  serverId: number
): [[] | Room[], { isLoading: boolean }] => {
  const [rooms, setRooms] = useState<[] | Room[]>([]);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    setIsLoading(true);
    (async () => {
      try {
        if (serverId) {
          const { data } = await axios.get(
            `${API_ENDPOINT}/servers/${serverId}/rooms`
          );
          console.log(data);
          setRooms(data.rooms);
        }
      } catch (error) {
        console.error(error);
      }
    })();
    setIsLoading(false);
  }, [serverId]);

  return [rooms, { isLoading }];
};