var CodeMirrorView = require('../views/codemirror');

module.exports = {
	getView: function(filesCollection){
    codeMirrorView = new CodeMirrorView();

    return codeMirrorView;
  }
}