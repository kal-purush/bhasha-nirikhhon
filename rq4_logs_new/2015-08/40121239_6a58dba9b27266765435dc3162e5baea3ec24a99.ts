/// <reference path="itemtypes.ts" />
/// <reference path="../../typings/all.ts" />

var fs = require('fs')
var path = require('path')

// taken from the node .d.ts
interface Stats {
    isFile(): boolean;
    isDirectory(): boolean;
    isBlockDevice(): boolean;
    isCharacterDevice(): boolean;
    isSymbolicLink(): boolean;
    isFIFO(): boolean;
    isSocket(): boolean;
    dev: number;
    ino: number;
    mode: number;
    nlink: number;
    uid: number;
    gid: number;
    rdev: number;
    size: number;
    blksize: number;
    blocks: number;
    atime: Date;
    mtime: Date;
    ctime: Date;
}

enum ItemTypes {
    File,
    Dir
}

class DirItem {
    path:string
    basename:string
    id:string
    stat: Stats
    type:ItemTypes
    selected:boolean

    constructor(fullpath:string) {
        this.path = fullpath
        this.basename = path.basename(fullpath)
        this.id = _.uniqueId("dir_item_")
        this.stat = fs.statSync(fullpath)
        if (this.stat.isDirectory()) {
            this.type = ItemTypes.Dir
        } else {
            this.type = ItemTypes.File
        }
    }

    type_string() {
        return this.type == ItemTypes.File ? "file" : "dir"
    }

    html_class() {
        return "item_" + this.type_string() + " item " + this.selected_text()
    }

    toggleSelected():void {
        this.selected = !this.selected
    }

    private selected_text():String {
        return this.selected ? "selected" : "unselected"
    }
}