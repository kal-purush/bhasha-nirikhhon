const express = require('express');
const app = express();
const wifi = require('node-wifi');

app.get('/', (req, res) => {
  res.send('Hello World!')
})

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`)
})



wifi.init({
	iface: null
});

wifi.scan((error, networks) => {
	if(error)
		console.log(error)
	else {
		console.log(networks);
	}
});