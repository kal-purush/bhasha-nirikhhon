import { LocalStore, SlotDay } from "@/types/store";

/**
 * Get copied week of slots from clipboard
 * @param state Local Redux State
 * @returns week of slots or null
 */
export const getWeekFromClipboard = (
  state: LocalStore
): LocalStore["copyPaste"]["week"] => state.copyPaste.week;

/**
 * Get copied slots day from clipboard
 * @param state Local Redux State
 * @returns slot dat or empty object
 */
export const getDayFromClipboard = (state: LocalStore): SlotDay =>
  state.copyPaste.day || {};