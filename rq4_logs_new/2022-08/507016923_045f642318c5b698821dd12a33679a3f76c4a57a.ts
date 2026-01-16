import { runInAction } from 'mobx';

import Config from '../../../../../../builds/dev-generated/Config';

import S from '../utilities/Main';
import StorageHelper from '../helpers/StorageHelper';
import WorkerQueueHelper, { Runnable } from './WorkerQueueHelper';

export default class ImagePreviewHelper {

    private static instance: ImagePreviewHelper = null;
    public static UNKNOWN_PREVIEW_URL = `${Config.URL.RESOURCES}/common/img/file-preview/unknown.svg`;

    workerQueueHelper: WorkerQueueHelper;
    callbacksQueueMap: Map < string, ((a_: string, b_: string) => void)[] >;

    private constructor(workerQueueHelper: WorkerQueueHelper) {
        this.workerQueueHelper = workerQueueHelper;
        this.callbacksQueueMap = new Map();
    }

    public static getSingletonInstance(workerQueueHelper: WorkerQueueHelper) {
        if (ImagePreviewHelper.instance === null) {
            ImagePreviewHelper.instance = new ImagePreviewHelper(workerQueueHelper);
        }

        return ImagePreviewHelper.instance;
    }

    public fetch(url: string, callback: (a_: string, b_: string) => void) {
        setTimeout(() => {
            if (this.callbacksQueueMap.has(url) === true) {
                this.callbacksQueueMap.get(url).push(callback);
                return;
            }

            const callbacks = [callback];
            this.callbacksQueueMap.set(url, callbacks);

            const storageHelper = StorageHelper.getSingletonInstance();
            const nftImageCacheModel = storageHelper.getNftImageCache(url);

            if (nftImageCacheModel !== null) {
                this.invokeCallbacks(url, nftImageCacheModel.mimeType, nftImageCacheModel.previewUrl);
                return;
            }

            this.workerQueueHelper.pushAndExecute(new Runnable(async () => {
                const res = await fetch(url);
                return res.headers.get('content-type');
            }, (type: string | null) => {
                const resultType = type ?? S.Strings.EMPTY;
                const resultPreviewUrl = ImagePreviewHelper.getPreviewUrlByType(url, resultType);

                storageHelper.addNftImageCache(url, resultType, resultPreviewUrl);
                this.invokeCallbacks(url, resultType, resultPreviewUrl);
            }));
        });
    }

    private invokeCallbacks(url, resultType, resultPreviewUrl) {
        runInAction(() => {
            this.callbacksQueueMap.get(url).forEach((callback: (a_: string, b_: string) => void) => {
                callback(resultType, resultPreviewUrl);
            });

            this.callbacksQueueMap.delete(url);
        });
    }

    public static getPreviewUrlByType(url: string, type: string) {
        if (type.indexOf('svg') !== -1) {
            return `${Config.URL.RESOURCES}/common/img/file-preview/svg.svg`
        } if (type.indexOf('mpeg') !== -1 || type.indexOf('mp3') !== -1 || type.indexOf('wav') !== -1 || type.indexOf('ogg') !== -1) {
            return `${Config.URL.RESOURCES}/common/img/file-preview/music.svg`
        } if (type.indexOf('mp4') !== -1 || type.indexOf('webm') !== -1 || type.indexOf('webp') !== -1) {
            return `${Config.URL.RESOURCES}/common/img/file-preview/video.svg`
        } if (type.indexOf('application') !== -1 || type.indexOf('gltf') !== -1 || type.indexOf('glb') !== -1) {
            return `${Config.URL.RESOURCES}/common/img/file-preview/gl.svg`
        } if (type.indexOf('jpeg') !== -1 || type.indexOf('jpg') !== -1 || type.indexOf('png') !== -1 || type.indexOf('gif') !== -1) {
            return url;
        }
        return ImagePreviewHelper.UNKNOWN_PREVIEW_URL;

    }

}