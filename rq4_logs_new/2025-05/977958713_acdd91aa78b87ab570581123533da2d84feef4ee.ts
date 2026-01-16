import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export type SocialLink = {
  id: string;
  platform: string;
  url: string;
  icon: string;
};

export type WebsiteLink = {
  id: string;
  title: string;
  url: string;
  description?: string;
};

interface ProfileState {
  avatar: string;
  name: string;
  bio: string;
  socialLinks: SocialLink[];
  websiteLinks: WebsiteLink[];
  // 操作方法
  setAvatar: (url: string) => void;
  addSocialLink: (link: SocialLink) => void;
  removeSocialLink: (id: string) => void;
  addWebsiteLink: (link: WebsiteLink) => void;
  removeWebsiteLink: (id: string) => void;
}

export const useProfileStore = create<ProfileState>()(
  persist(
    (set) => ({
      avatar: '/avatar.png',
      name: '我的主页',
      bio: '欢迎来到我的个人空间',
      socialLinks: [],
      websiteLinks: [],

      setAvatar: (url) => set({ avatar: url }),
      addSocialLink: (link) =>
        set((state) => ({
          socialLinks: [...state.socialLinks, link],
        })),
      removeSocialLink: (id) =>
        set((state) => ({
          socialLinks: state.socialLinks.filter((link) => link.id !== id),
        })),
      addWebsiteLink: (link) =>
        set((state) => ({
          websiteLinks: [...state.websiteLinks, link],
        })),
      removeWebsiteLink: (id) =>
        set((state) => ({
          websiteLinks: state.websiteLinks.filter((link) => link.id !== id),
        })),
    }),
    { name: 'profile-storage' }
  )
);