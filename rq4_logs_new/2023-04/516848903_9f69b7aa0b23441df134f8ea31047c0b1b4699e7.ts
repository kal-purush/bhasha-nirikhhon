import { ReactNode } from "react";

export type DrawerProps = {
  children?: ReactNode;
  /**
   * a string that will be applied as a css class to parent element
   */
  className?: string;
  open: boolean;
  onClose: () => void;
  placement?: "top" | "bottom" | "left" | "right";
};