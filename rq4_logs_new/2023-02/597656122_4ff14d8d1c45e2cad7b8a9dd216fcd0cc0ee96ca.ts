import { Room } from "@/types/room";
import axios from "axios";
import { useCallback, useState } from "react";

type Props = {
  roomIds: string[];
  deep: number;
};

export const useGetRooms = () => {
  const [allRooms, setAllRooms] = useState<undefined | Room[]>();
  const fetchAllRooms = useCallback(
    async (props: Props) => {
      const response = await axios.get<Room[]>('/api/room', {
        params: {
          roomIds: props.roomIds,
          deep: props.deep
        }
      });
      setAllRooms(response.data);
    }, []
  );

  return { allRooms, setAllRooms, fetchAllRooms };
};