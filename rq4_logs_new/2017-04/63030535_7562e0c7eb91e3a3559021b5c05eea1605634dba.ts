"use strict";

/**
 * Creates and caches gulp-typescript projects per project.
 */
export const projectFactory = ((): (fileName: string) => any => {
    const projects: { [i: string]: any } = {};

    return (fileName: string): any => {
        const ts = require("gulp-typescript");

        return projects[fileName] = ts.createProject(fileName);
    };
})();