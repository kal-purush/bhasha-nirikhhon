var path = require('path')
var fs = require('fs')
var marked = require('marked')
var REPO_URL = 'https://github.com/CodeHubOrg/javascript101/blob/master/docs/'

// var render = function(filesArr, res, vars){
//   contentsArr = []
//   var numFiles = filesArr.length
//   filesArr.map(function(filename){    
//     var file = path.join(__dirname, '../docs' ,filename)
//     fs.readFile(file, 'utf8', function(err, contents){
//       var contentsObj = {
//         content: marked(contents),
//         editUrl: REPO_URL+filename
//       }
//       err ? console.error :
//       contentsArr.push(contentsObj)
//       if(contentsArr.length == numFiles){
//         console.log(contentsArr)
//         if(contentsArr[0].editUrl == 'https://github.com/CodeHubOrg/javascript101/blob/master/docs/roadmap.md'){
//           contentsArr = [contentsArr[1], contentsArr[0]]
//         }
//         res.render('getstarted', {contentblocks: contentsArr, active:vars.active})
//       }
//     })
//   })
// }

var render = function(dirPath, res, vars){
  var dir =  path.join(__dirname,'..', dirPath)
  contentsArr = []
  fs.readdir(dir, function(err, list){
     if(err){
      console.error("list could not be read:", err)
     } else {
        var len = list.length
        list.forEach(function(filename){
          var file = path.join(dir, filename)
          fs.readFile(file, 'utf8', function(err, contents){
            heading = filename.replace('-',' ').replace('.md','').replace(/\b\w/g, function(l){ return l.toUpperCase() }) //use all one regular expr!
            var contentsObj = {
              content: marked(contents),
              heading: heading,
              filename: file
            }
            err ? console.error : 
            contentsArr.push(contentsObj)
            if(contentsArr.length == len){
              res.render('subpages', {listNum: len, contentBlocks: contentsArr, content: vars.header1})
            }
          })
        })
       
     }
  })
}  


module.exports = {
  render: render
}