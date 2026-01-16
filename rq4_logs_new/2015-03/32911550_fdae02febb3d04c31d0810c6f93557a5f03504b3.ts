declare module tsFsPromise {

    interface OpenOptions {
        encoding?: string;
        flag?: string;
    }

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

    interface Main {
        readJSON(path:string):Promise<any>;
        readFile(path:string, encoding:string):Promise<any>;
        copy(src:string, dest:string):Promise<boolean>;
        createFile(file:string):Promise<boolean>;
        mkdirs(dir:string):Promise<boolean>;
        mkdirp(dir:string):Promise<boolean>;
        outputFile(file: string, data: any):Promise<boolean>;
        outputJSON(file: string, data: any):Promise<boolean>;
        remove(dir: string):Promise<boolean>;
        writeJSON(file: string, object: any, options?: tsFsPromise.OpenOptions):Promise<boolean>;
        rename(oldPath: string, newPath: string):Promise<boolean>;
        stat(path: string):Promise<tsFsPromise.Stats>;
        writeFile(filename: string, data: any, encoding?: string):Promise<boolean>;
    }

}

declare module "ts-fs-promise" {
    var foo:tsFsPromise.Main;
    export = foo;
}