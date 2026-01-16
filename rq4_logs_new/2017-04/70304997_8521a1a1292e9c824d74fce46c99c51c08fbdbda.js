module.exports = (grunt) => {

  grunt.initConfig({
    pkg : grunt.file.readJSON('package.json'),

    concurrent : {
      dev : {
        tasks : ['nodemon', 'exec:watchify'],
        options : {
          logConcurrentOutput : true
        }
      }
    },

    nodemon : {
      dev : {
        script : 'app.js'
      }
    },

    exec : {
      watchify : 'npm run watch'
    }

  });

    //Task plugin load.
    grunt.loadNpmTasks('grunt-nodemon');
    grunt.loadNpmTasks('grunt-exec');
    grunt.loadNpmTasks('grunt-concurrent')
    //Task definition: Task name + plugins to run.
    grunt.registerTask('server', ['concurrent:dev']);
}