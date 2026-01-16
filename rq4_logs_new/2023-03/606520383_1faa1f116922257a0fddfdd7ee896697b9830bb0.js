import * as nodePath from 'path';
const rootFolder = nodePath.basename(nodePath.resolve());

const buildFolder = `./dist`;
const srcFolder = `./src`;

export const path = {
    build: {
        fonts: `${buildFolder}/fonts/`,
        images: `${buildFolder}/img/`,
        svg: `${buildFolder}/img/`,
        js: `${buildFolder}/js/`,
        css: `${buildFolder}/css/`,
        html: `${buildFolder}/`,
        files: `${buildFolder}/files/`
    },
    src: {
        fonts: `${srcFolder}/fonts/**/*.{woff,woff2}`,
        images: `${srcFolder}/img/**/*.{jpg,webp,jpeg,png,gif}`,
        svg: `${srcFolder}/img/**/*.svg`,
        js: `${srcFolder}/js/app-p.js`,
        scss: `${srcFolder}/scss/*.scss`,
        html: `${srcFolder}/*.html`,
        files: `${srcFolder}/files/**/*.*`,
    },
    watch: {
        images: `${srcFolder}/img/**/*.{jpg,webp,ico,svg,jpeg,png,gif}`,
        svg: `${srcFolder}/img/**/*.svg`,
        js: `${srcFolder}/js/**/*.js`,
        scss: `${srcFolder}/scss/**/*.scss`,
        html: `${srcFolder}/**/*.html`,
        files: `${srcFolder}/files/**/*.*`,
    },
    clean: buildFolder,
    buildFolder: buildFolder,
    srcFolder: srcFolder,
    rootFolder: rootFolder,
    ftp: ``
}