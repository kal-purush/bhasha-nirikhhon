import { create } from "zustand";
import { FightingRoomDTO } from "../DTOs/room/FightingRoom.dto";

// MemberDTO를 상태로 갖는 store를 생성합니다.
interface FightingRoomStore {
  fightingRoom: FightingRoomDTO | undefined;
  setFightingRoom: (fightingRoom: FightingRoomDTO) => void;
}

// Zustand를 사용하여 상태와 setter 함수를 생성합니다.
const useFightingRoomStore = create<FightingRoomStore>((set) => ({
  fightingRoom: undefined,
  setFightingRoom: (fightingRoom) => set({ fightingRoom }),
}));

export default useFightingRoomStore;