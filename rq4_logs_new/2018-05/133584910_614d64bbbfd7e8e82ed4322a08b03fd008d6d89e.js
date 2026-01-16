const stealTools = require('steal-tools');

stealTools.build({}, {})
  .then(() => console.log('done'))
  .catch(e => console.error(e));