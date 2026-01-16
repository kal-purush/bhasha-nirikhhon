/**
 * This file is only a proof of concept on an idea on how to make it work. 
 * The file will probably change a lot.
 * 
 * What do I want to improve on this: 
 *      * Templating on responses
 *      * Socket.io for live view
 *      * Maybe even add a master/node(s) architecture on this
 */

/**
 * We need access to the filesystem, md5 hashing library and express framework
 */
const chokidar = require('chokidar');
const express = require('express');

var app = express();
/**
 * We want date and time in our console window
 */
require('log-timestamp');

const Watcher = chokidar.watch('data.txt');

const log = console.log.bind(console);

var hasFileChanged = false

Watcher.on('change', function(filepath) {
  // At this point the file has changed
  hasFileChanged = true
});

app.get('/', (req, res, next) => {
  res.json(["Welcome to the file watching API"]);
});

app.get('/changed', (req, res, next) => {
  if (hasFileChanged) {
    res.json(["File has been changed"]);
  } else {
    res.json(["File has not been changed"]);
  }
});

/**
* Let us start the server and listen on the port provided by the configuration file
*/
app.listen(8080, () => {
  console.log(`Server running on port 8080`);
});