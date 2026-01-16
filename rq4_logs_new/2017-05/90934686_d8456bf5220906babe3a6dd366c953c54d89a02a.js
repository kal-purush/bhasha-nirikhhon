// Simple plugin that converts every identifier to "LOL"
function lolizer() {
  return {
    visitor: {
      Identifier(path) {
        path.node.name = 'LOL';
      }
    }
  }
}
Babel.registerPlugin('lolizer', lolizer);

var output = Babel.transform(
  'function helloWorld() { alert(hello); }',
  {plugins: ['lolizer']}
);

console.log(output.code)