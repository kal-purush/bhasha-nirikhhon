/// <reference path="../_references.ts" />
/// <reference path="../_references.ts" />

module Extropy {
    export interface IAssetManager extends IDisposable {
        load(): Q.Promise<void>;
        getAsset<T>(assetName: string): Q.Promise<T>;
    }
}