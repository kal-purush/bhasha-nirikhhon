module.exports = function(grunt) {
  grunt.loadNpmTasks('grunt-war');

  grunt.initConfig({

        /*
         * Build a WAR (web archive) without Maven or the JVM installed.
         */
        war: {
          target: {

            options: {
                war_dist_folder: 'public',
                war_verbose: true,
                war_name: 'adminTool',
                webxml_welcome: 'index.html',
                webxml_display_name: 'adminTool',
                webxml_mime_mapping: [{
                    extension: 'woff',
                    mime_type: 'application/font-woff'
                }]/*,
                webxml_webapp_extras: function() {
                  return "<servlet><servlet-name>default</servlet-name><servlet-class>org.apache.catalina.servlets.DefaultServlet</servlet-class><init-param><param-name>debug</param-name><param-value>0</param-value></init-param><init-param><param-name>listings</param-name><param-value>true</param-value></init-param><load-on-startup>1</load-on-startup></servlet><servlet-mapping><servlet-name>default</servlet-name><url-pattern>/profileadminapp/adminTool</url-pattern></servlet-mapping>";
                }*/
            },
            files: [{
                expand: true,
                cwd: 'public',
                src: ['**'],
                dest: ''
            }]
          }
        }
  });

};