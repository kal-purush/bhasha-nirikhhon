'use strict;

module.exports = function(grunt){
    
    // load all grunt contrib modules
    require("matchdep").filterDev("grunt-*").forEach(grunt.loadNpmTasks);
    
    // project configuration
    grunt.initConfig({
        
        // load config file
        pkg: grunt.file.readJSON('package.json'),

        
        
    }); // end of configuration
    
    // register development task
    grunt.registerTask('dev', ['']);
    
    // register distribution task
    grunt.registerTask('dist', ['']);
    
}; // end of module