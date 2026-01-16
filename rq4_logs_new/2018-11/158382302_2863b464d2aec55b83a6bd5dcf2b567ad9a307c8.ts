import { BookMeta } from "./book";

export type Library = {
    [key: string]: BookMeta | undefined;
};