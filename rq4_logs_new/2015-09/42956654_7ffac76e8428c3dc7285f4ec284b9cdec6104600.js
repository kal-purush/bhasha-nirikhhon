module.exports = function(grunt) {
    grunt.initConfig({
        ts: {
            options: {
                target: 'es5',
                verbose: true,
                ast: 'never',
                sourceMap: true,
                declaration: true
            },

            VizUmlTs: {
                files: [
                    {
                        src: ['scripts/VizTs/*.ts', 'scripts/VizUmlTs/*.ts'],
                        dest: 'build/vizumlts.js'
                    }
                ]
            }
        },

        uglify: {
            options: {
                banner: '/*! VizUmlTs <%= grunt.template.today("yyyy-mm-dd") %> */\n',
                
                sourceMap: true
            },

            VizUmlTs: {
                
                    src: 'build/vizumlts.js',
                    dest: 'build/vizumlts.min.js'
                
            }
        }
    });

    grunt.loadNpmTasks("grunt-ts");
    grunt.loadNpmTasks("grunt-contrib-uglify");
    grunt.registerTask("default", ["ts", "uglify"]);
};