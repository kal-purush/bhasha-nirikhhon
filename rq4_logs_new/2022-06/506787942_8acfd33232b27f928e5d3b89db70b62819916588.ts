import { MouseEventHandler } from "react";

export interface Contact {
  icon: string,
  color: string,
  text: string,
  link?: string,
  onclick?: MouseEventHandler
}