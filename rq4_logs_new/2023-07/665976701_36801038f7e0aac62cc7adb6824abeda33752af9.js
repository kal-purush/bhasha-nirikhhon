const fs = require('fs').promises;
const path = require('path');

const inputPath = process.argv[2];
if (!inputPath) {
    console.error('Please provide an input path (file or directory) as an argument');
    process.exit(1);
}

const outputFile = path.resolve(__dirname, 'mergedChests.json');

async function processFile(inputFile) {
    try {
        const data = await fs.readFile(inputFile, 'utf8');
        const jsonData = JSON.parse(data);
        let newChests;

        if (jsonData['mapFeature.customPois']) { 
            newChests = jsonData['mapFeature.customPois']
                .filter(poi => poi.name.startsWith('Loot Chest'))
                .map(poi => poi.location);
        } else {
            newChests = jsonData.chests || [];
        }

        let outputData;
        try {
            const data = await fs.readFile(outputFile, 'utf8');
            outputData = JSON.parse(data);
        } catch (err) {
            if (err.code === 'ENOENT') {
                outputData = {
                    "points": [],
                    "chests": [],
                    "notes": [],
                    "date": ""
                };
            } else {
                console.error(`Error reading output file: ${err}`);
                return;
            }
        }

        const chestSet = new Set(outputData.chests.map(JSON.stringify));
        newChests.forEach(chest => {
            const chestString = JSON.stringify(chest);
            if (!chestSet.has(chestString)) {
                outputData.chests.push(chest);
                chestSet.add(chestString);
            }
        });

        outputData.date = new Date().toLocaleString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: true
        });

        await fs.writeFile(outputFile, JSON.stringify(outputData, null, 2));
        console.log(`File ${outputFile} has been updated.`);
    } catch (err) {
        console.error(`Error processing file ${inputFile}: ${err}`);
    }
}

async function processPath(inputPath) {
    try {
        const stats = await fs.lstat(inputPath);

        if (stats.isDirectory()) {
            const files = await fs.readdir(inputPath);
            files.filter(file => path.extname(file).toLowerCase() === '.json')
                .reduce((p, file) => p.then(() => processFile(path.join(inputPath, file))), Promise.resolve());
        } else if (stats.isFile()) {
            if (path.extname(inputPath).toLowerCase() !== '.json') {
                console.error('Input file must be a .json file');
                process.exit(1);
            }
            await processFile(inputPath);
        } else {
            console.error('Input path is neither a file nor a directory');
            process.exit(1);
        }
    } catch (err) {
        console.error(`Error processing input path: ${err}`);
        process.exit(1);
    }
}

processPath(inputPath)
    .catch(err => console.error(`Error: ${err}`));