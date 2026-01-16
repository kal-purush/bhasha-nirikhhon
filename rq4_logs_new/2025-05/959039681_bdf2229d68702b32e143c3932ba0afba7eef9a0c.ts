import { getCookieValue } from "@/lib/cookies";
import { create } from "zustand";

interface SidebarState {
  isCollapsed: boolean;
  toggleCollapse: () => void;
}

export const useSidebarStore = create<SidebarState>((set) => {
  const defaultState = getCookieValue("sidebar_state");
  const isCollapsedFromCookie = defaultState === "false";
  return {
    isCollapsed: isCollapsedFromCookie,
    toggleCollapse: () =>
      set((state) => ({
        isCollapsed: !state.isCollapsed,
      })),
  };
});