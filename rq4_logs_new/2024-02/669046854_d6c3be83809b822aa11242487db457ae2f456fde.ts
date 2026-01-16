import { AiOutlineHome } from "react-icons/ai";
import { RiComputerLine } from "react-icons/ri";
import { FaCaretRight } from "react-icons/fa";
import { NavItem } from "@/components/common/home/home-side-nav/config/sidenav-interface";
import { MdDashboard } from "react-icons/md";
import { LuCircle } from "react-icons/lu";

//This is the config for the side nav items
export const navConfig: NavItem[] = [
  {
    title: "Home",
    href: "#",
    icon: AiOutlineHome,
  },
  {
    title: "Dashboard",
    href: "/table",
    icon: MdDashboard,
    subNav: [
      {
        title: "Sub Nav 1",
        href: "#",
        icon: LuCircle,
        subNav: [
          {
            title: "Sub Nav 1",
            href: "#",
            icon: LuCircle,
          },
          {
            title: "Sub Nav 2",
            href: "#",
            icon: LuCircle,
          },
          {
            title: "Sub Nav 3",
            href: "#",
            icon: LuCircle,
          },
        ],
      },
      {
        title: "Sub Nav 2",
        href: "#",
        icon: LuCircle,
      },
      {
        title: "Sub Nav 3",
        href: "#",
        icon: LuCircle,
      },
    ],
  },
  {
    title: "Products",
    href: "/",
    icon: RiComputerLine,
    subNav: [
      {
        title: "Products List",
        href: "/",
        icon: FaCaretRight,
      },
      {
        title: "Table View",
        href: "/table",
        icon: FaCaretRight,
      },
      {
        title: "Order Page",
        href: "/orders",
        icon: FaCaretRight,
      },
    ],
  },
];

// This is used for the side nav styling
export const sideNavBgColorDesktop =
  "bg-[url('/sidebar/sidebar-bg2.svg')] border-r border-dotted border-[#0ac31c]/20 dark:border-[#7BFE88]/20";
export const sideNavBgColorMobile =
  "bg-[url('/layout/admin-bg-white.svg')] dark:bg-[url('/layout/admin-bg.svg')] bg-no-repeat bg-cover bg-fixed border-r border-dotted border-[#0ac31c]/20 dark:border-[#7BFE88]/20";
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