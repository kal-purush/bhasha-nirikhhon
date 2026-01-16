import MyWorkbench from "@/views/Workbench/MyWorkbench.vue";

const routes = [
  {
    path: "/workbench",
    name: "MyWorkbench",
    meta: {
      title: "工作台",
      icon: "desktop",
      i18n: "workbench",
      role: ["admin"],
    },
    component: MyWorkbench,
  },
];

export default routes;