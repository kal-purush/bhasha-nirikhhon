/** A minimal abstraction of FileSystem operations needed to provide a cache proxy for 'glob'. None of the methods are expected to throw. */
export interface GlobFileSystem {
    /** Returns `true` if the specified `path` is a directory, `undefined` if it doesn't exist and `false` otherwise. */
    isDirectory(path: string): boolean | undefined;
    /** Returns `true` if the specified `path` is a symlink, `false` in all other cases. */
    isSymbolicLink(path: string): boolean;
    /** Get the entries of a directory as string array. */
    readDirectory(dir: string): string[];
    /** Get the realpath of a given `path` by resolving all symlinks in the path. */
    realpath(path: string): string;
}

export interface NodeLikeFileSystem {
    statSync(path: string): { isDirectory(): boolean; };
    lstatSync(path: string): { isSymbolicLink(): boolean; };
    readdirSync(dir: string): string[];
    realpathSync(path: string): string;
}

export function convertNodeLikeFileSystem(fs: NodeLikeFileSystem): GlobFileSystem {
    return {
        isDirectory(path) {
            try {
                return fs.statSync(path).isDirectory();
            } catch {
                return;
            }
        },
        isSymbolicLink(path) {
            try {
                return fs.lstatSync(path).isSymbolicLink();
            } catch {
                return false;
            }
        },
        readDirectory(dir) {
            return fs.readdirSync(dir);
        },
        realpath(path) {
            return fs.realpathSync(path);
        }
    }
}

export function memoizeFileSystem(fs: GlobFileSystem): GlobFileSystem {
    const isDirectoryCache = new Map<string, boolean | undefined>();
    const isSymbolicLinkCache = new Map<string, boolean>();
    const readDirectoryCache = new Map<string, string[]>();
    const realpathCache = new Map<string, string>();

    return {
        isDirectory(path) {
            let result = isDirectoryCache.get(path);
            if (result === undefined && !isDirectoryCache.has(path)) {
                result = fs.isDirectory(path);
                isDirectoryCache.set(path, result);
            }
            return result;
        },
        isSymbolicLink(path) {
            let result = isSymbolicLinkCache.get(path);
            if (result === undefined) {
                result = fs.isSymbolicLink(path);
                isSymbolicLinkCache.set(path, result);
            }
            return result;
        },
        readDirectory(dir) {
            let result = readDirectoryCache.get(dir);
            if (result === undefined) {
                result = fs.readDirectory(dir);
                readDirectoryCache.set(dir, result);
            }
            return result;
        },
        realpath(path) {
            let result = realpathCache.get(path);
            if (result === undefined) {
                result = fs.realpath(path);
                realpathCache.set(path, result);
            }
            return result;
        },
    }
}

const directoryStats = {
    isDirectory() { return true; },
};
const otherStats = {
    isDirectory() { return false; },
};

const propertyDescriptor = { writable: true, configurable: true };

export function createGlobInterceptor(fs: GlobFileSystem) {
    const cache = new Proxy<Record<string, false | 'FILE' | string[]>>({}, {
        getOwnPropertyDescriptor() {
            return propertyDescriptor;
        },
        get(_, path: string) {
            switch (fs.isDirectory(path)) {
                case true:
                    return fs.readDirectory(path);
                case false:
                    return 'FILE';
                default:
                    return false;
            }
        },
        set() {
            // ignore cache write
            return true;
        },
    });
    const statCache = new Proxy<Record<string, any>>({}, {
        get(_, path: string) {
            switch (fs.isDirectory(path)) {
                case true:
                    return directoryStats;
                case false:
                    return otherStats;
                default:
                    return false;
            }

        },
        set() {
            // ignore cache write
            return true;
        },
    });
    const realpathCache = new Proxy<Record<string, string>>({}, {
        getOwnPropertyDescriptor() {
            return propertyDescriptor;
        },
        get(_, path: string) {
            return fs.realpath(path);
        },
    });
    const symlinks = new Proxy<Record<string, boolean>>({}, {
        getOwnPropertyDescriptor() {
            return propertyDescriptor;
        },
        get(_, path: string) {
            return fs.isSymbolicLink(path);
        },
    });
    return {
        cache,
        realpathCache,
        statCache,
        symlinks,
    };
}