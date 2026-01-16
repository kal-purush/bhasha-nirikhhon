'use strict';

require("coffee-script/register");
var util = require("util");
var file = require("file");
var linguist = require("atom-linguist");
var Report = require("./report.js");

var Languist = module.exports = {
  
  /*
  * identify
  *
  * Identifies the language for a single file
  *
  * @param file
  *   The file you would like to identify
  *
  * @returns callback(error, language)
  *   Where error is a potential error and language
  *   the identified language
  */
  identify: function(file, callback) {
    try {
      var language = linguist.detect(file);
      
      // When it returns multiple languages, grab the last one
      if(language instanceof Array) {
        language = language[language.length -1];
      }
      
      callback(null, language);
    } catch(e) {
      callback(e);
    }
  },
  
  /*
  * identify
  *
  * Identifies the language for a single file
  *
  * @param file
  *   The file you would like to identify
  *
  * @returns language
  *   The identified language
  */
  identifySync: function(file) {
     try {
      var language = linguist.detect(file);
      
      // When it returns multiple languages, grab the last one
      if(language instanceof Array) {
        language = language[language.length -1];
      }
        return language;
      } catch(e) {
        throw(e);
    }
  },
  contains: function(array, object) {
      var i = array.length;
      while (i--) {
        if (array[i] === object) {
           return true;
        }
      }
    return false;
  },  
  
  /*
  * sortIgnore
  *
  * Formats the given array and sorts it in 
  * files and folders
  *
  * @param ignore 
  *   The array containing the ignored folder names, file names or file types
  */
  sortIgnore: function(ignore) {
     
    var ignored = {
      files: [],
      directories: []
    };
    
    for(var i = 0; i < ignore.length; i++) {
      
      if(ignore[i].match(/\*(.)+/g) || ignore[i].match(/([^\s]+(\.)[^\s]+)/)) {
                
        // If we're talking about a file type        
        if(ignore[i].charAt(0) == "*") {
          ignored.files.push(ignore[i].substr(1));  
        } else {
          // File name         
          ignored.files.push(ignore[i]);
        }
      } else {
        ignored.directories.push(ignore[i]);
      }        
    }
   
    return ignored;
  },
  
  /*
  * isIgnored
  *
  * Checks if a file or folder is on the ignore list
  *
  * @param fileOrFolder
  *   The file or folder to be checked against the respective array of ignored files/folders
  *
  * @param ignored
  *   The ignored folder or files array
  */
  isIgnored: function(fileOrFolder, ignored) {
   
    for(var i = 0; i < ignored.length; i++) {
      
      if(fileOrFolder == ignored[i]) {
        return true;
      }
      
      if(fileOrFolder.search(ignored[i]) > 0) {
         
        /*
        * This part fixes for example that *.js does not ignore json files
        */ 
        if(ignored[i].match(/(^(\.)[^\s]+)/)) {
           var parts = fileOrFolder.split(".");
           var fileType = ignored[i].split(".");
           
           if(parts[parts.length - 1] == fileType[1]) {
             return true;
           }
        } else {
          return true;
        }
      }
    }
    
    return false;
  },  
  
  /*
  * walkIdentifySync
  *
  * Identifies an entire directory recursively
  * 
  * @param folder
  *   The name of the folder you would like to identify
  *
  * @param ignore
  *   The array containing the ignored folder names, file names or file types
  *
  * @returns Report
  *   Returns the Report object
  */
  walkIdentifySync: function(folder, ignore) {
    
    // Format the given ignored files/directories seperately
    var ignored = typeof(ignore) != "undefined" ? Languist.sortIgnore(ignore) : null;
    var report = new Report();
    
    // Loop through the given folder      
    file.walkSync(folder, function(dirPath, dirs, files) {    
      
      // Skip ignored directories
      if(ignored == null || !Languist.isIgnored(dirPath, ignored.directories)) {     
        files.forEach(function(file) {
          
          // Skip ignored files
          if(ignored == null || !Languist.isIgnored(file, ignored.files)) {
                       
            var language = Languist.identifySync(dirPath + "/" + file);
            
            // Filter out not recognized files
            if(typeof(language) != "undefined") {
              report.files.push(file);
              
              if(typeof(language) != "undefined")
              
              // Only add language once
              if(!Languist.contains(report.languages, language))
                report.languages.push(language);
             
              // occurrences for statistics
              if(!report.occurrences[language]) {
                report.occurrences[language] = 1;
              } else {
                report.occurrences[language] += 1;
              }
            }
          }
        });  
      }
    });
    
    return report;
  }
};