import fs from 'fs'
import path from 'path';
import { fileURLToPath } from 'url';
import { v4 as uuid } from 'uuid';

const __filename = fileURLToPath(import.meta.url); // get the resolved path to the file
const __dirname = path.dirname(__filename); // get the name of the directory

const dirInputs = path.join(__dirname, "inputs");

if(!fs.existsSync(dirInputs)) {
    fs.mkdirSync(dirInputs, {recursive: true});
}

const generateInputFile = (input) => {
    const jobId = uuid();
    const inputFileName = `${jobId}.txt`
    const inputFilePath = path.join(dirInputs, inputFileName);
    fs.writeFileSync(inputFilePath , input);
    return inputFilePath;
}

export default generateInputFile