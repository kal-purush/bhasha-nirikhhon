'use strict';
var yeoman = require('yeoman-generator');
var chalk = require('chalk');
var yosay = require('yosay');

module.exports = yeoman.Base.extend({

    prompting: function() {
      var done = this.async();
      this.prompt({
        type: 'input',
        name: 'name',
        message: 'Your project name',
        //Defaults to the project's folder name if the input is skipped
        default: this.appname
      }, function(answers) {
        this.props = answers
        done();
      }.bind(this));
    },

  writing: function () {
      this.fs.copyTpl(this.templatePath('package.json'), this.destinationPath('package.json'), {name: this.props.name});
      this.fs.copy(this.templatePath('.babelrc'), this.destinationPath('.babelrc'));
      this.fs.copy(this.templatePath('.eslintrc.json'), this.destinationPath('.eslintrc.json'));
      this.fs.copy(this.templatePath('src/index.js'), this.destinationPath('src/index.js'));
  },

  install: function () {
    this.npmInstall();
  }
});