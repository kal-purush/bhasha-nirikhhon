const cprint = require('color-print');
const path = require('path');
const fs = require('fs');

export class Changeset {
    public readonly paths: Array<string>;
    public readonly changeList: Array<string>;

    constructor (pathsArray: Array<string>) {
        this.paths = pathsArray;
        this.changeList = this.paths.forEach( path => this.findWarden(path) );
    }

    public mapChangedPaths (changedFiles: Array<string>): Array<string> {
        const changedPaths = changedFiles.map((file:any) => path.dirname(file)).filter(this.onlyUnique);

        return changedPaths;
    }

    private findWarden (in_directory: string = './'): string | false {
        let directory = path.resolve(process.cwd(), in_directory);
        let wardenFile = path.resolve(directory, '.warden');
    
        const maxUpwardsIteration = 100;
        let loopCount = 0;
    
        while (true) {
            if (fs.existsSync(wardenFile)) {
                break;
            }
    
            const oldDirectory = directory;
            directory = path.dirname(directory);
            if (directory === oldDirectory) {
                break;
            }
    
            if (loopCount++ > maxUpwardsIteration) {
                cprint.yellow('Too many loop iterations! Invalid top directory: ' + directory);
                break;
            }
    
            wardenFile = path.resolve(directory, '.warden');
        }
    
        if (! fs.existsSync(wardenFile)) {
            return false;
        }
    
        return wardenFile;
    }

    private onlyUnique(value: any, index: number, self: Array<string>) {
        return self.indexOf(value) === index;
    }
}