// Package imports
//TODO: if I end up not using this just remove this
import React, { FC } from "react";
// non- package import
import { Toy } from "appTypes";

export interface WindowContextValues {
  isNonMobileWidth: boolean;
  isNonMobileHeight: boolean;
  isLaptopWidth: boolean;
  isLaptopHeight: boolean;
}

export const WindowContext = React.createContext<WindowContextValues>({
  isNonMobileWidth: false,
  isNonMobileHeight: false,
  isLaptopWidth: false,
  isLaptopHeight: false,
});

export interface ModalReduxFormContextValues {
  onModalClose: VoidFunction | null;
  toy?: Toy;
  [x: string]: any;
}

export const ModalReduxFormContext =
  React.createContext<ModalReduxFormContextValues>({
    onModalClose: null,
  });