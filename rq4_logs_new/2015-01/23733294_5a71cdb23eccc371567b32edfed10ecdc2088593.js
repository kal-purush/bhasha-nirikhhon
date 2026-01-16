module.exports = {
  dist : {
      src: ['<%= cwd %>/lib/**/*.js'], 
      options: {
          destination: '<%= cwd %>/docs',
          template : "node_modules/grunt-jsdoc/node_modules/ink-docstrap/template",
          configure : "<%= cwd %>/jsdoc.conf.json"
      }
  }
}