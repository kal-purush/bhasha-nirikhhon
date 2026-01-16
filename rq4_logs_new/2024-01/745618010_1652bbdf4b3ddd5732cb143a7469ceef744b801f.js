import fs from "fs";
import path from "path";

const directory = "src/files";

(function main() {
  if (!fs.existsSync(directory)) {
    console.error(`Directory "${directory}" not found`);
    return;
  }

  // Read the directory contents
  fs.readdir(directory, (err, files) => {
    if (err) {
      console.error(`Error reading the directory: ${err}`);
      return;
    }

    // Iterate over each file
    files.forEach((file) => {
      let fullPath = path.join(directory, file);

      // Check if it's a file
      fs.stat(fullPath, (err, stats) => {
        if (err) {
          console.error(`Error getting file stats: ${err}`);
          return;
        }

        if (stats.isFile()) {
          // Delete the file
          fs.unlink(fullPath, (err) => {
            if (err) {
              console.error(`Error deleting file: ${err}`);
            }
          });
        }
      });
    });
  });
})();