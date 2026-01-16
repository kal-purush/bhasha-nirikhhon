/// <reference path="src/d.ts/allClient.d.ts" />

//var packageJson = require('package.json');

(function() {
    //grunt tasks loaded directly from node_modules.  Custom tasks loaded from tasks directory
    module.exports = function(grunt) {

        //# set base path so all subsequent paths are relative to the root of the repo
        //grunt.file.setBase(path.resolve('..'));
        var mountFolder = function(connect, dir) {
            return connect.static(require('path').resolve(dir));
        }

        grunt.initConfig({
            pkg: grunt.file.readJSON('package.json'),
            project: {
                src: {
                    devServer: 'src/server',
                    devClient: 'src'
                },
                dest: {
                    devServer: 'server',
                    devClient: 'public'
                },
                banner: '/**\n* <%= pkg.title || pkg.name %> v<%= pkg.version %>\n*\n* Copyright (c) 2015\n* <%= pkg.author %>; Boxer Technology, L.L.C.\n*\n* Licensed <%=pkg.license %.\n*\n* This software and all information contained herein is the property\n* of Boxer Technology, L.L.C. Much of this information including ideas,\n* concepts, formulas, processes, data, know-how, techniques, and\n* the like, found herein is considered proprietary to Boxer Technology, L.L.C.,\n* and may be covered by U.S. and foreign patents or patents pending,\n* or protected under trade secret laws.  Any dissemination, disclosure,\n* use, or reproduction of this material for any reason inconsistent with\n* the express purpose for which it has been disclosed is strictly\n* forbidden.\n**/\n'
            }

        });

        grunt.loadTasks('grunt');

        grunt.registerTask('serve', ['connect:livereload', 'watch:server', 'watch:sass']);

    }

}).call(this);