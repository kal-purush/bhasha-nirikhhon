import { atom } from "recoil";

export const AlertModalOpen = atom({
    key: "alertModalOpen",
    default: {
        isOpen: false,
        whatAlert: 100
    }
});