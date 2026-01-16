const IssueModel = require('./issue.model');

function init(connection) {
  [
    IssueModel,
  ].forEach((model) => model.init(connection));
}

module.exports = {
  init,
};