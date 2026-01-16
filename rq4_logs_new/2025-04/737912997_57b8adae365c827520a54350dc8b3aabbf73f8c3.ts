import { getStringEnv } from "@/helpers/get-string-env";
import { appRoutes } from "@/constants/routes";
import { envKeys } from "@/enums/env-keys";

export const navigationItems = [
    {
        link: appRoutes.home,
        id: "Home",
        imageName: "home.png",
    },
    {
        link: `/${getStringEnv(envKeys.address003)}`,
        id: getStringEnv(envKeys.addressName003),
        imageName: "73-8.svg",
    },
    {
        link: `/${getStringEnv(envKeys.address004)}`,
        id: getStringEnv(envKeys.addressName004),
        imageName: "75-1.svg",
    },
    {
        link: `/${getStringEnv(envKeys.address005)}`,
        id: getStringEnv(envKeys.addressName005),
        imageName: "75-3.svg",
    },
    {
        link: `/${getStringEnv(envKeys.address002)}`,
        id: getStringEnv(envKeys.addressName002),
        imageName: "68a-63.svg",
    },
    {
        link: `/${getStringEnv(envKeys.address001)}`,
        id: getStringEnv(envKeys.addressName001),
        imageName: "1-12.svg",
    },
] as const;