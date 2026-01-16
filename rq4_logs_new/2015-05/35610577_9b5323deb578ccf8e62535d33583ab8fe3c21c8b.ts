/// <reference path="./loader_config.ts" />
/// <reference path="./loader_util.ts" />
/// <reference path="./loader_extractor.ts" />
/// <reference path="./loader_core.ts" />
/// <reference path="./wwa_data.ts" />

declare function postMessage(message: any): void;
declare function postMessage_noWorker(message: any): void;
function sendToMain(m: any): void {
    if (loader_conf.conf.is_worker) {
        postMessage(m);
    } else {
        postMessage_noWorker({
            data: m
        });
    }
}

import util = loader_util;
import WWAConsts = loader_wwa_data.WWAConsts;
import WWAData = loader_wwa_data.WWAData;
import WWADataExtractor = loader_extractor.WWADataExtractor;
import WWALoader = loader_core.WWALoader;



function loader_start( e: MessageEvent ): void {
     if (e.data.fileName !== void 0) {
        var loader: WWALoader = new WWALoader(e.data.fileName);
        loader.requestMapData();
    } else {
        var resp = new loader_wwa_data.LoaderResponse();
        resp.wwaData = null;
        resp.progress = null;
        resp.error = new loader_wwa_data.LoaderError();
        resp.error.name = "";
        resp.error.message = "マップデータのファイル名が指定されていません。";

        sendToMain(resp);
    }   
}

if (loader_conf.conf.is_worker) {
    addEventListener("message", loader_start);
}