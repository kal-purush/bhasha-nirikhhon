const uuid = require('uuid');

const lights = { lights: []};
for(i=0; i<64; ++i) {
	var id = uuid.v4();
	id = id.substr(1, id.indexOf("-") - 1)
	lights.lights.push( { id, address: i} );
}

console.log(JSON.stringify(lights));