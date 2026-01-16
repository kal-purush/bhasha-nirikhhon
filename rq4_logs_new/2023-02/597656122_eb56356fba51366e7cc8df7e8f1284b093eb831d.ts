import { FrontRoom, Room } from "@/types/room";

export const tagsParse = (room: Room): FrontRoom => {
    const tags = room.tags.length ? room.tags.split(',') : [];

    return { ...room, tags };
};

export const tagsStringify = (frontRoom: FrontRoom): Room => {
    const tags = frontRoom.tags.join(',');

    return { ...frontRoom, tags };
};