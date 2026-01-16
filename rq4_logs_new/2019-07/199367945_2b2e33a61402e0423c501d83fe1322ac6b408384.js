const babel = require('@babel/core');

babel.transformFile('./test.jsx', {
	sourceType: "module",

	plugins: [
		// "@babel/plugin-syntax-jsx",
		"@babel/plugin-transform-react-jsx",
		"./jsx-traverse-test",
	]
}, (err, result) => {
  if (err) {
    console.log('---something wrong---');
    console.log(err);
    return;
  }

  console.log('---result code---');
  console.log(result.code);
});