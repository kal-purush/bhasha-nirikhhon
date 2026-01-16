import axios from "axios";
import { Dayjs } from "dayjs";
import { iRoomCards } from "../interfaces/interfaces";
import { Meeting } from "../interfaces/interfaces";

const addMeeting = async (
    Name: string | undefined,
    Description: string | undefined,
    StartDate: Dayjs,
    EndDate: Dayjs,
    Participants: number[] | undefined
) => {
    return await axios.post("http://localhost:3001/meetings", {
        meeting_name: Name,
        meeting_description: Description,
        startDate: StartDate,
        endDate: EndDate,
        participants: Participants,
    });
};

const getMeetingsData = async () => {
    return await axios.get("http://localhost:3001/roomdata");
};

const getAllRooms = async (): Promise<iRoomCards[]> => {
    return (await axios.get(`http://localhost:3001/rooms/`)).data;
};

const getMeetings = async (): Promise<Meeting[]> => {
    const result = await axios.get("http://localhost:3001/meetings");
    return result.data;
};

export { addMeeting, getMeetings, getMeetingsData, getAllRooms };