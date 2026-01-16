import { atom } from "recoil";

export type CalendarPopUpInfoType = {
  lawsuit: {
    id: number;
    name: string;
    num: string;
    type: string;
    commissionFee: number;
    contingentFee: number;
  };
  clients: { id: number; name: string }[];
  members: { id: number; name: string }[];
  reception: {
    id: number;
    status: boolean;
    contents: string;
    category: string;
    receivedAt: string;
    deadline: string;
  };
};

const calendarPopUpInfoState = atom<CalendarPopUpInfoType | null>({
  key: "calendarPopUpInfoState",
  default: null,
});

export default calendarPopUpInfoState;