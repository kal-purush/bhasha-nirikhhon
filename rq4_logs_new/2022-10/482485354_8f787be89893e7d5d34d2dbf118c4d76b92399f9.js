import React from "react";
import Quest1 from "@quest-1/app/Quest1";
import Quest2 from "@quest-2/app/Quest2";

export const MenuItems = [
  {
    itemName: "Quest-1",
    route: "/quest-1",
    component: <Quest1 />,
  },
  {
    itemName: "Quest-2",
    route: "/quest-2",
    component: <Quest2 />,
  },
];