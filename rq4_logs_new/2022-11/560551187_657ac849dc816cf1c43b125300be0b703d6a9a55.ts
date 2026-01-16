import React from "react";

export type str = string; 
export type bool = boolean; 
export type num = number; 
export type eventChange = React.ChangeEvent<HTMLInputElement> | React.ChangeEvent<HTMLSelectElement>; 
export type onClick = React.MouseEvent<HTMLElement>; 
export type formEvent = React.FormEvent<HTMLFormElement>; 