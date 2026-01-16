var path = require('path');
var gulp = require('gulp');
var install = require('gulp-install');
var conflict = require('gulp-conflict');
var template = require('gulp-template');
var inquirer = require('inquirer');
var notifier = require('node-notifier');
var slug = require('slug');
var titlecase = require('titlecase');
var projectName = titlecase(path.basename(process.cwd()));
var projectSlug = slug(projectName).toLowerCase();



gulp.task('default', function(cb) {

  inquirer.prompt([
    {
      type: 'input',
      name: 'name',
      message: 'Project name',
      default: projectName
    },
    {
      type: 'input',
      name: 'subdomain',
      message: 'Preview subdomain',
      default: projectSlug
    },
    {
      type: 'confirm',
      name: 'confirm',
      message: 'Continue?'
    }
  ], function(res) {
    return scaffold(res, cb)
  });

});



function scaffold(res, cb) {

  if(!res.confirm) {
    return cb();
  }

  gulp.src(__dirname + '/templates/**')
    .pipe(template(res))
    .pipe(conflict('./'))
    .pipe(gulp.dest('./'))
    .pipe(install())
    .on('end', function() {
      notifier.notify({
				title: projectName + ' is ready to go!',
				message: 'Your project is setup and ready to go.'
			});
      cb();
    })
    .resume();

}