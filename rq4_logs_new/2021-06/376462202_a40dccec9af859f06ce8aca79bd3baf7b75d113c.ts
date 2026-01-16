import Webpack, { Configuration } from 'webpack';
import HTMLPlugin from "html-webpack-plugin";
import { join } from 'path';
import { existsSync } from 'fs';

export function createWebpackConfiguration(baseApplicaationDirectory: string, mode: Configuration['mode']) {
    const plugins: Configuration['plugins'] = [];

    //If the project has an index.html configured
    if (existsSync(join(baseApplicaationDirectory, "public", "index.html"))) {
        plugins.push(new HTMLPlugin({
            template: join(baseApplicaationDirectory, "public", "index.html")
        }))
    }
    
    return Webpack({
        mode,
        context: baseApplicaationDirectory,
        output: {
            //Let's write to the build directory as react already does
            path: join(baseApplicaationDirectory, "build"),
            libraryTarget: "system",
            filename: "index.js"
        },
        entry: {
            main: "./src/index.ts"
        },
        plugins
    })
}