/// <reference path="interface.ts" />
/// <reference path="../typings/tsd.d.ts" />
/// <reference path="vash.d.ts" />
import fs = require("fs");
import vash = require("vash");
import grunt = require("grunt");
export function createByCssOutput(cssFiles: ICssFile[], outputFolder:string): void {
    createOutputFolder(outputFolder);
    var output:string = render("templates/byCssFile.vash", cssFiles);
    grunt.file.write(outputFolder + "byCss.html", output);
}

function render(templatePath: string, model: any): string {
    var byCssFileTemplate: string = grunt.file.read(templatePath);
    var templateFunction: (model: any) => string = vash.compile(byCssFileTemplate);
    return templateFunction(model);
}

function createOutputFolder(folderPath:string) {
    if (!fs.existsSync(folderPath)) {
        fs.mkdirSync(folderPath);
    }
}