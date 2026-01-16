import { ACESHIP_RESOURCE_REPOSITORY, HELLA_RESOURCE_REPOSITORY, RESOURCE_REPOSITORY } from "..";
import { ExcelTables } from "../../../../types/impl/lib/impl/local";
import type { Skin, Skins } from "../../../../types/impl/lib/impl/skins";
import { get as getSkins } from "../../local/impl/get";

export const get = async (id: string): Promise<Skin | null> => {
    const skins = await getAll();
    const skin = skins.find((skin) => skin.id === id) ?? null;
    return skin;
};

export const getSkinsByCharacter = async (id: string): Promise<Skin[]> => {
    const skins = await getAll();
    return skins.filter((skin) => skin.charId === id);
};

export const getAll = async (): Promise<Skin[]> => {
    const data = (await getSkins(ExcelTables.SKIN_TABLE)) as Skins;

    const results: Skin[] = [];

    for (const skin in data.charSkins) {
        let eliteImage = "";
        if (data.charSkins[skin].displaySkin.skinGroupId) {
            switch (data.charSkins[skin].displaySkin.skinGroupId) {
                case "ILLUST_0": {
                    eliteImage = `https://raw.githubusercontent.com/${ACESHIP_RESOURCE_REPOSITORY}/main/ui/elite/0.png`;
                    break;
                }
                case "ILLUST_1": {
                    eliteImage = `https://raw.githubusercontent.com/${ACESHIP_RESOURCE_REPOSITORY}/main/ui/elite/1.png`;
                    break;
                }
                case "ILLUST_2": {
                    eliteImage = `https://raw.githubusercontent.com/${ACESHIP_RESOURCE_REPOSITORY}/main/ui/elite/2.png`;
                    break;
                }
                case "ILLUST_3": {
                    eliteImage = `https://raw.githubusercontent.com/${ACESHIP_RESOURCE_REPOSITORY}/main/ui/elite/3.png`;
                    break;
                }
                default: {
                    eliteImage = `https://raw.githubusercontent.com/${HELLA_RESOURCE_REPOSITORY}/main/skingroups/${encodeURIComponent(data.charSkins[skin].displaySkin?.skinGroupId?.split("#")[1])}.png`;
                    break;
                }
            }
        }
        Object.assign(data.charSkins[skin], { id: skin, image: `https://raw.githubusercontent.com/${RESOURCE_REPOSITORY}/main/skin/${encodeURIComponent(data.charSkins[skin].portraitId)}b.png`, eliteImage });
        results.push(data.charSkins[skin]);
    }
    return results;
};