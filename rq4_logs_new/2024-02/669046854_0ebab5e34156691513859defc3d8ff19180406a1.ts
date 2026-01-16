// This is used for the side nav styling
import { globalBgConfig } from "@/app/global-bg-config";

export const sideNavBgColorDesktop =
  "bg-[url('/sidebar/sidebar-bg2.svg')] bg-no-repeat bg-cover bg-fixed border-r border border-[#e5e7eb]/10 ";
export const sideNavBgColorMobile = `${globalBgConfig} bg-no-repeat bg-cover bg-fixed border-r border border-[#e5e7eb]/10 `;
//This is used for the nav item styling
export const activeNavItemClasses =
  "dark:bg-[#18333d] bg-[#b3dafd] w-full rounded-md border-l-4 my-0 dark:border-[#7BFE88]  border-[#0ac31c] text-black dark:text-[#58afd6] text-black text-2xl font-bold";

//This is used for sub nav items styling
export const activeSubNavItemClasses =
  "dark:bg-[#182b3d] bg-[#a1c4e4] w-full rounded-md border-l-4 my-1.5 border-[#7BFE88] text-black dark:text-[#58afd6] text-black text-lg font-bold";

//This is used for sub nav title styling (font size, font weight, margin)
export const subNavItemTitleFontClasses = "ml-2 text-sm font-medium";

//This is the heading name for the side nav
export const headingName = "RSAdmin";

// This is used for the heading styling
export const headingClasses = "text-2xl font-bold text-black dark:text-white";

export const defaultTextClasses =
  "text-[#637381] dark:text-[#919eab] text-xl font-bold";