import {ensureIsReExportFile} from "./ensure-is-re-export-file";
import * as path from "path";
import * as fs from "fs";
import {groupBy} from "./utils";

export class ReExportBuilder {
    private directoryGroups: { key: string; values: string[] }[];
    private finishedCount: number = 0;

    // make private once typescript supports it
    constructor(globMatches: string[], private finishedCallback?: () => void) {
        this.setGlobMatchesToDirectoryGroups(globMatches);

        this.directoryGroups.forEach((group) => {
            this.handleFolderWithFiles(group.key, group.values);
        });
    }

    static createReExportFiles(globMatches: string[], finishCallback?: () => void) {
        return new ReExportBuilder(globMatches, finishCallback);
    }

    private setGlobMatchesToDirectoryGroups(globMatches: string[]) {
        let groups = groupBy(globMatches, (item) => path.dirname(item));
        let mainParentDirectory = this.getMainParentDirectory(groups);
        this.directoryGroups = groups.filter(groups => groups.key !== mainParentDirectory).sort((a, b) => b.key.length - a.key.length); // length descending
    }

    private getMainParentDirectory(groups: { key: string; values: string[] }[]) {
        let shortestDir = groups.length > 0 ? groups[0].key : "";

        groups.forEach(group => {
            if (group.key.length < shortestDir.length) {
                shortestDir = group.key;
            }
        });

        return shortestDir;
    }

    private handleFolderWithFiles(folderName: string, files: string[]) {
        const reExportFilePath = path.join(folderName, "..", path.basename(folderName) + ".ts");

        fs.readFile(reExportFilePath, "utf8", (err, data) => {
            this.handleFileWithData({
                reExportFilePath: reExportFilePath,
                data: data,
                files: files
            });
        });
    }

    private handleFileWithData(obj: { reExportFilePath: string; data: string; files: string[]; }) {
        if (ensureIsReExportFile(obj.data)) {
            this.writeReExportFile(obj);
        }
        else {
            console.warn(`Skipping "${obj.reExportFilePath}". The file did not only contain export statements.`);
        }

        this.addReExportFileToDirectoryGroups(obj.reExportFilePath);
    }

    private writeReExportFile({ reExportFilePath, files }: { reExportFilePath: string; files: string[]; }) {
        fs.writeFile(reExportFilePath, this.getFileTextFromFiles(files), (err) => {
            this.onFileFinished();
        });
    }

    private getFileTextFromFiles(fileNames: string[]) {
        return fileNames.map(name => `export * from "./${path.basename(path.dirname(name)) + "/" + path.parse(name).name}";`).join("\n") + "\n";
    }

    private addReExportFileToDirectoryGroups(reExportFilePath: string) {
        const baseGroup = path.dirname(reExportFilePath);
        const baseGroupIndex = this.directoryGroups.map((g) => g.key).indexOf(baseGroup);

        if (baseGroupIndex >= 0) {
            this.directoryGroups[baseGroupIndex].values.push(reExportFilePath);
        }
    }

    private onFileFinished() {
        this.finishedCount++;

        if (this.finishedCount === this.directoryGroups.length && typeof this.finishedCallback === "function") {
            this.finishedCallback();
        }
    }
}