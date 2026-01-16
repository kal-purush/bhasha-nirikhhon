var fs = require ('fs');
var ohm = require ('ohm-js');

var argv = require('yargs/yargs')(process.argv.slice(2)).argv;
var drawio = fs.readFileSync (argv._[0], 'UTF-8');

// npm install atob
var atob = require ('atob');

var grammar = String.raw`
DrawioWithTabs {
  main = mxFileBegin diagram+ mxFileEnd

  mxFileBegin = "<mxFile" ~">"+ ">"
  mxFileEnd = "</mxfile>"

  diagram = "<diagram" ~">"+ ">"
}
`;

var fab = String.raw`
DrawioWithTabs {
  main [mxFileBegin diagram+ mxFileEnd] = ‛«mxFileBegin»«diagram»«mxFileEnd»’
  mxFileBegin [kopen cs+ kclose] = ‛«kopen»«cs»«kclose»’
  mxFileEnd [kclose] = ‛«kclose»’
  diagram [kopen cs+ kclose] = ‛«kopen»«cs»«kclose»’
}
`;

function uncompress (s) {
    var data = atob(s);

    // npm install pako
    var pako = require ('pako');
    
    var inf = pako.inflateRaw (
	Uint8Array.from (data, c=>c.charCodeAt (0)), {to: 'string'})
    var str = decodeURIComponent (inf);
    return str;
}