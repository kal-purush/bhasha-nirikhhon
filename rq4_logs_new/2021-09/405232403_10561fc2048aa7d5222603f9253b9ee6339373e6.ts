// Package imports
import { ThunkDispatch } from "redux-thunk";
import { AnyAction } from "redux";
// Non-package imports
import { RootState } from "../reducers/index";

// aggregate these modules for easy importing outside
export * from "./aliasInterfaces";
export * from "./authActions";
export * from "./toysActions";
export * from "./errorActions";
export * from "./loaderActions";
export * from "./wikiSearchActions";
export * from "./types";

export type AppDispatch = ThunkDispatch<RootState, any, AnyAction>;