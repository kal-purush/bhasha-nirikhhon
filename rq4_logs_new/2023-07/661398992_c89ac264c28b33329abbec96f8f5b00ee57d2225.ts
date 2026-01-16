import React from "react";

// Props
export type ToggleStateType = {
  toggle: boolean;
  setToggle: React.Dispatch<React.SetStateAction<boolean>>;
};

export type OffmenuProps = {
  item: MenuItem;
};

export type MenuItem = {
  key: number;
  title: string;
  link: string;
  subLinks?: {
    key: number;
    title: string;
    link: string;
  }[];
};