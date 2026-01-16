import * as React from "react";
function HelloMessage(name: string)
{
  return React.createClass({
    displayName: "HelloMessage",
    render: function() {
      return React.createElement("div", null, "Hello ", name);
    }
  })
}



// var jeact = require('jaect');
//  fs = require('fs'), expect = require('chai').expect,  esprima = require('esprima-fb'), escodegen = require('escodegen-ts')
//
// var root = __dirname + '/reactcases/'
//
// describe('ReactCase', function () {
//   var files = fs.readdirSync(root)
//   files.forEach(createTest)
// })