import { atom } from "recoil";

const snbLoadedState = atom({
  key: "snbLoadedState",
  default: false,
});

export default snbLoadedState;