const fs = require('fs');
const path = require('path');
const imgDir = path.join(__dirname, 'img'); // 假设img文件夹位于项目根目录
const outputFilePath = path.join(__dirname, 'images.json');

function isImageFile(filePath) {
    const ext = path.extname(filePath).toLowerCase();
    return ['.jpg', '.jpeg', '.png', '.gif'].includes(ext);
}

function walkDir(dir, filelist = []) {
    const files = fs.readdirSync(dir);

    files.forEach(function (file) {
        const filepath = path.join(dir, file);
        const stat = fs.statSync(filepath);
        if (stat.isDirectory()) {
            filelist = walkDir(filepath, filelist);
        } else if (isImageFile(filepath)) {
            const relativePath = path.relative(imgDir, filepath);
            filelist.push({
                src: `https://cdn.jsdelivr.net/gh/ddabb/goodpic/img/${relativePath.split(path.sep).join('/')}`,
                alt: path.basename(file, path.extname(file))
            });
        }
    });

    return filelist;
}

function createImagesJson() {
    const images = walkDir(imgDir);
    const jsonData = JSON.stringify(images, null, 4);
    fs.writeFileSync(outputFilePath, jsonData, 'utf-8');
    console.log(`Generated ${outputFilePath}`);
}

createImagesJson();