import Dashboard from "@/assets/icons/dashboard.svg?react";
import Team from "@/assets/icons/team.svg?react";
import Projects from "@/assets/icons/projects.svg?react";
// import Calendar from "@/assets/icons/calendar.svg?react";
// import Document from "@/assets/icons/document.svg?react";
// import Reports from "@/assets/icons/reports.svg?react";

// 1:admin
// 2:user
const navList = [
  {
    id: "1a",
    name: "داشبورد",
    href: "/admin",
    index: true,
    icon: Dashboard,
    role: "admin",
  },
  {
    id: "2a",
    name: "تیم",
    href: "/admin/team",
    icon: Team,
    role: "admin",
  },
  {
    id: "1u",
    name: "داشبورد",
    href: "/user",
    index: true,
    icon: Dashboard,
    role: "user",
  },
  {
    id: "3a",
    name: "پروژه‌ها",
    href: "/admin/projects",
    icon: Projects,
    role: "admin",
  },
  //   {
  //     id: 4,
  //     name: "تقویم",
  //     href: "/admin/calendar",
  //     icon: Calendar,
  //   },
  //   {
  //     id: 5,
  //     name: "گزارش‌ها",
  //     href: "/admin/report",
  //     icon: Reports,
  //   },
  //   {
  //     id: 6,
  //     name: "اسناد",
  //     href: "/admin/document",
  //     icon: Document,
  //   },
];

export default navList;