import { getStoredValue } from "./store-helper";
import { announcementsCountKey } from "../config/store";

export function hasNewAnnouncements(announcementRowsCount: number): boolean {
    return (
        announcementRowsCount > 0 &&
        getStoredValue(announcementsCountKey) !== announcementRowsCount
    );
}