// Clean up the domains file for the haley-js-npm.
const fs = require('fs');
const path = require('path');

const RED = '\x1b[31m%s\x1b[0m';

const INSTALL_PATH_TO_PROJECT_FOLDER = process.env.npm_config_install_path_to_project_folder || '../../../../';

// if COPY_DOMAINS_TO_FOLDER is not set, then the domains will be copied to lib-vital/vitalservice;
let FOLDERS_TO_BE_CLEAN;

const COPY_TO_PATH = path.join(__dirname, INSTALL_PATH_TO_PROJECT_FOLDER);
FOLDERS_TO_BE_CLEAN = [
    path.join(COPY_TO_PATH, 'vitalservice/domains'),
    path.join(COPY_TO_PATH, 'vitalservice/domains-data'),
];


console.log('Cleaning up domains files');
FOLDERS_TO_BE_CLEAN.forEach(function(dir) {
    if (!fs.existsSync(dir)){
        console.log(`directory: ${dir} does not exist`);
    } else {
        fs.readdir(dir, (err, files) => {
            if (err) throw err;
          
            for (const file of files) {
              console.log(`Deleting file ${path.join(dir, file)}`);
              fs.unlink(path.join(dir, file), err => {
                if (err) throw err;
              });
            }
          });
    }
});