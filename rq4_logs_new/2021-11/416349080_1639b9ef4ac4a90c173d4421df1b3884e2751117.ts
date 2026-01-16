import LoggedLayout from "./LoggedLayout";
import GuestLayout from "./GuestLayout";
import { LayoutComponentType } from "./types";

export enum Layouts {
  Guest,
  Logged,
}

const layouts = new Map<Layouts, LayoutComponentType>();

layouts.set(Layouts.Guest, GuestLayout);
layouts.set(Layouts.Logged, LoggedLayout);

const defaultLayout = LoggedLayout;

export function getLayout(name?: Layouts): LayoutComponentType {
  if (name === undefined) return defaultLayout;

  return layouts.get(name) ?? defaultLayout;
}