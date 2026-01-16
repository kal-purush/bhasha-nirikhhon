const {execSync} = require('child_process');
const fs = require('fs-extra');
const path = require('path');
const SimpleGit = require('simple-git/promise');
let simpleGit = SimpleGit();
const cheerio = require('cheerio');
const prettier = require("prettier");

const stdWrite = function (message) {
    process.stdout.write(message);
}

const gitFolder = 'robot-basketball-rules';
const generatedFolder = 'docs';
const rulesFilePrefix = 'basketball_rules_';
const languages = ['est', 'eng'];
const translations = {
    compare: {est: 'Võrdle', eng: 'Compare'},
    source: {est: 'Lähtefail', eng: 'Source file'},
    repository: {est: 'Kõik lähtefailid', eng: 'Repository'},
}
const githubRepoURL = 'https://github.com/ut-robotics/robot-basketball-rules';

function cleanAll() {
    cleanGitRepo();
    fs.rmdirSync(generatedFolder, {recursive: true});
}

function cleanGitRepo() {
    fs.rmdirSync(gitFolder, {recursive: true});
}

async function generate() {
    cleanAll();

    stdWrite(`clone ${githubRepoURL} ... `);
    await simpleGit.clone(`${githubRepoURL}.git`);
    stdWrite(`done\n`);

    simpleGit = SimpleGit(gitFolder);

    const tags = (await simpleGit.tags()).all;

    fs.mkdirSync(generatedFolder);

    for (const tag of tags) {
        await generateHTMLForTag(tag, tags);
    };

    stdWrite(`generate index.html ... `);
    generateIndexHTML(tags);
    stdWrite(`done\n`);

    cleanGitRepo();
}

async function generateHTMLForTag(activeTag, tags) {
    fs.mkdirSync(path.join(generatedFolder, activeTag));

    stdWrite(`checkout ${activeTag} ... `);
    await simpleGit.checkout(activeTag);
    stdWrite(`done\n`);

    for (const lang of languages) {
        const adocFileName = `${rulesFilePrefix}${lang}.adoc`;
        const htmlFileName = `${rulesFilePrefix}${lang}.html`;
        const htmlFilePath = path.join(generatedFolder, activeTag, htmlFileName);

        stdWrite(`generate ${htmlFilePath} ... `);

        execSync(`asciidoctor ${path.join(gitFolder, adocFileName)} -o ${htmlFilePath}`);

        await fs.copy(path.join(gitFolder, 'images'), path.join(generatedFolder, activeTag, 'images'));

        const $ = cheerio.load(fs.readFileSync(htmlFilePath));

        $('body').prepend(`<div id="top-links">
            ${renderTags(activeTag, tags, lang)}
            <a id="source-link" href="${githubRepoURL}/blob/${activeTag}/${adocFileName}">${translations.source[lang]}</a>
            <a id="repo-link" href="${githubRepoURL}">${translations.repository[lang]}</a>
            <div class="languages">${languages.map(l => renderLang(l, lang, activeTag)).join('')}</div>
        </div>`);

        $('head').append(`<style>
            #top-links {
                font-family: "Open Sans", "DejaVu Sans", sans-serif;
                border-bottom: 1px solid #eee;
                display: flex;
                flex-wrap: wrap;
                justify-content: space-between;
            }

            #top-links a {
                text-decoration: none;
            }

            #top-links a:hover {
                text-decoration: underline;
            }

            #tags {
                display: flex;
                flex-grow: 1;
            }

            #tags > * {
                margin-right: 20px;
            }

            #top-links a,
            #top-links .languages *,
            #source-link,
            #repo-link,
            #top-links .tag-name {
                display: inline-block;
                padding: 10px;
            }
        </style>`)

        fs.writeFileSync(htmlFilePath, $.html());

        stdWrite(`done\n`);
    };
}

function renderLang(lang, activeLang, tag) {
    if (lang === activeLang) {
        return `<span>${lang}</span>`;
    }

    return `<a href="/${tag}/${rulesFilePrefix}${lang}.html">${lang}</a>`;
}

function renderTags(activeTag, tags, lang) {
    return `<div id="tags">${tags.map(t => renderTag(t, activeTag, lang)).join('')}</div>`
}

function renderTag(tag, activeTag, lang) {
    const isActive = tag === activeTag;

    const olderTag = tag < activeTag ? tag : activeTag;
    const newerTag = olderTag === tag ? activeTag : tag;

    return `<div>
        <span class="tag">${isActive ? `<span class="tag-name">${tag}</span>` : `<a href="/${tag}/${rulesFilePrefix}${lang}.html">${tag}</a>`}</span>
        <span>${isActive ? '' : `<a href="${githubRepoURL}/compare/${olderTag}...${newerTag}">${translations.compare[lang]}</a>`}</span>
    </div>`;
}

function generateIndexHTML(tags) {
    function renderTagLinks(tag) {
        return `<div>
            <h1>${tag}</h1>
            ${languages.map(lang => `<div>${renderTagLangLink(tag, lang)}</div>`).join('\n')}
            </div>`
    }

    function renderTagLangLink(tag, lang) {
        return `<a href="/${tag}/${rulesFilePrefix}${lang}.html">${lang}</a>`;
    }

    const html = `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Robot Basketball Rules</title>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Open+Sans:300,300italic,400,400italic,600,600italic%7CNoto+Serif:400,400italic,700,700italic%7CDroid+Sans+Mono:400,700">

            <style>
                html, body {
                    margin: 0;
                    padding: 0;
                }

                body {
                    display: flex;
                    flex-direction: column;
                    justify-content: center;
                    align-items: center;
                    color: rgba(0,0,0,.85);
                }

                body, h1 {
                    font-family: "Open Sans", "DejaVu Sans", sans-serif;
                    font-weight: 300;
                }

                #title {
                    font-weight: 400;
                }

                a {
                    color: #2156a5;
                    text-decoration: none;
                    font-size: 2em;
                }

                a:hover {
                    text-decoration: underline;
                }
            </style>
        </head>
        <body>
        <div>
        <h1 id="title">Basketball Rules</h1>
        ${tags.map(t => renderTagLinks(t)).join('\n')}
        </div>
        </body>
        </html>`;

    fs.writeFileSync(path.join(generatedFolder, 'index.html'), prettier.format(html, {parser: 'html'}));
}

generate();