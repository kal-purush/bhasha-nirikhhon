module.exports = function(client, scope, audience, context, cb) {
	var access_token = {};
	access_token.scope = scope; // do not remove this line

	const namespace = 'https://ataper.net/';
	switch(client.id) {
	case "${anonymous_client_id}":
		access_token[namespace + 'role'] = "anonymous";
		break
	case "${rules_client_id}":
		access_token[namespace + 'role'] = "internal";
		break
	}

	cb(null, access_token);
}