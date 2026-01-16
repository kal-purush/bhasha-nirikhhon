import Node from '../../../Contracts/FileSystem/Nodes/Node';
import NodeCreator from '../../../Contracts/FileSystem/Nodes/NodeCreator';
import NodeRemover from '../../../Contracts/FileSystem/Nodes/NodeRemover';
import * as fs from 'fs';
import * as path from 'path';
import { CallBack } from 'promise-lib';
import { FileCreator } from './FileCreator';
import { FileRemover } from './FileRemover';




export class FileNode extends Node {

    constructor(protected _path: string, creator: NodeCreator, remover: NodeRemover) {
        super(creator, remover);
    }

    get path() {
        return this._path;
    }

    set path(v: string) {
        this._path = v;
    }


    async GetChildren() {
        let [err, files]: [Error, string[]] = await CallBack.Call(fs.readdir, this._path);
        if (err) throw err;

        return (files || []).map(file => {
            return new FileNode(path.join(this._path, file), this.creator, this.remover);
        })
    }


    static Create(_path:string){
        return new FileNode(_path,new FileCreator,new FileRemover);
    }

}