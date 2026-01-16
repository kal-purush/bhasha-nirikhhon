let fs = require('fs');
let path = require('path');
let glob = require('glob');
let HtmlWebpackPlugin = require('html-webpack-plugin');

const myTest = () => {
    console.log('\n\nmyTest =========================\n\n\n')
    const realContentFolderPath = path.resolve(__dirname + 'dist/pages/');
    const layout = fs.readFileSync(`src/templates/handlebars/index.handlebars`, { encoding: 'utf8' });

    const generatePage = template => {
        const pageContent = fs.readFileSync(template, { encoding: 'utf-8' });
        return layout.replace('{# PAGE_CONTENT #}', pageContent);
    }

    // const pages = glob.sync(contentDir + '/**/*.html');
    /* return pages.map(page => new HtmlWebpackPlugin({
        templateContent: generatePage(page),
        filename: page.replace(realContentFolderPath, ''),
        hash: true
    })); */

    return new HtmlWebpackPlugin({
        title: 'My Plugin DEmo page Example JS 1144',
        template: path.resolve(__dirname, '../src/templates/handlebars/index.handlebars'),
        filename: path.resolve(__dirname, '../dist/pages/test660000.html'), //relative to root of the application
    })
}

module.exports = myTest;