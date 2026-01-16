import * as Page from "OV/page";
import * as Messages from "OV/messages";
import * as VideoTypes from "video_types";
import * as Background from "Messages/background";
import * as Storage from "OV/storage";
import * as Environment from "OV/environment";

function _getPageRefData() {
    if(Environment.isExtensionPage(location.href)) {
        return null;
    }
    let host = location.href.match(/:\/\/(www\.)?([^/]*)\/?/)![2];
    let link = document.querySelector("link[rel='shortcut icon']") as HTMLLinkElement;
    if(link) {
        return { url: location.href, icon: Page.getAbsoluteUrl(link.href), name: host };
    }
    else {
        return { url: location.href, icon: "", name: host };
    }
}

export function setup() {
    Messages.addListener({
        getPageRefData: async function () {
            return _getPageRefData();
        }
    })
}
export async function getPageRefData() {
    if(Page.isFrame()) {
        let response = await Background.toTopWindow({ data: null, func: "getPageRefData" });
        return response.data as VideoTypes.PageRefData | null;
    }
    else {
        return _getPageRefData();
    }
}
(window as any)["getPageRefData"] = getPageRefData;
export async function convertOldPlaylists() {
    let oldfav = await Storage.local.get("OpenVideoFavorites") as VideoTypes.HistoryEntry[];
    let oldhist = await Storage.local.get("OpenVideoHistory") as VideoTypes.HistoryEntry[];
    let mapping = (el : VideoTypes.HistoryEntry) => {
        return {
            title: el.title,
            poster: el.poster,
            origin: {
                url: el.origin,
                name: "",
                icon: ""
            },
            parent: {
                url: "CONVERTED_FROM_OLD",
                name: "CONVERTED_FROM_OLD",
                icon: "CONVERTED_FROM_OLD"
            },
            watched: el.stoppedTime,
            duration: 0
        }
    };
    if(oldfav) {
        let newfav = oldfav.map(mapping);
        await Storage.setPlaylistByID(Storage.fixed_playlists.favorites.id, newfav);
        await Storage.local.set("OpenVideoFavorites", null)
    }
    if(oldhist) {
        let newhist = oldhist.map(mapping);
        await Storage.setPlaylistByID(Storage.fixed_playlists.history.id, newhist);
        await Storage.local.set("OpenVideoHistory", null)
    }
}