import { useStore } from "react-redux";
import { useSelector } from "react-redux";
import { useDispatch } from "react-redux";


export const uzeDispatch=useDispatch.withTypes()
export const uzeSelector=useSelector.withTypes()
export const uzeStore=useStore.withTypes()