import axiosInstance from "@/commons/axios";
import { create } from "zustand";
import { persist } from "zustand/middleware";
import {
  MemberMonthGrowth,
  MemberStateGrowth,
  RecentActivity,
  Stat,
} from "@/types/stat";

interface StatStore {
  stats: Stat | null;
  membersGrowth: MemberMonthGrowth[];
  membersStateGrowth: MemberStateGrowth[];
  recentActivity: RecentActivity[];
  loading: boolean;
  getStats: () => Promise<void>;
  getMembersGrowth: () => Promise<void>;
  getMembersStateGrowth: () => Promise<void>;
  getRecentActivity: () => Promise<void>;
}

export const useStatStore = create<StatStore>()(
  persist(
    (set, get) => ({
      loading: false,
      stats: null,
      membersGrowth: [],
      membersStateGrowth: [],
      recentActivity: [],
      getStats: async () => {
        try {
          set({ loading: true });
          const response = await axiosInstance.get("/stats");
          set({ stats: response.data, loading: false });
        } catch (error) {
          set({ loading: false });
          throw error;
        }
      },

      getMembersGrowth: async () => {
        try {
          set({ loading: true });
          const response = await axiosInstance.get("/member-growth");
          set({ membersGrowth: response.data, loading: false });
        } catch (error) {
          set({ loading: false });
          throw error;
        }
      },

      getMembersStateGrowth: async () => {
        try {
          set({ loading: true });
          const response = await axiosInstance.get("/member-state-growth");
          set({ membersStateGrowth: response.data, loading: false });
        } catch (error) {
          set({ loading: false });
          throw error;
        }
      },

      getRecentActivity: async () => {
        try {
          set({ loading: true });
          const response = await axiosInstance.get("/recent-activities");
          set({ recentActivity: response.data, loading: false });
        } catch (error) {
          set({ loading: false });
          throw error;
        }
      },
    }),
    {
      name: "stat-store",
    }
  )
);