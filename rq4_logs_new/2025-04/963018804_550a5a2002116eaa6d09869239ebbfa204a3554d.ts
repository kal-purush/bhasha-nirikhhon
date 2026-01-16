import { SVGProps } from "react";

export interface IconProps extends SVGProps<SVGSVGElement> {
  width?: number;
  height?: number;
  stroke?: string;
  strokeWidth?: number;
  className?: string;
}