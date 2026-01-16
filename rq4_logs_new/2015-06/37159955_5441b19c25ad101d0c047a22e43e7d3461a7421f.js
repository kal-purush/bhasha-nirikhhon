var http = require("http"),
	https = require("https"),
	assert = require("assert"),
	crypto = require("crypto"),
	async = require('asyncawait/async'),
	await = require('asyncawait/await'),
	Promise = require('bluebird'),
	util = require("util");
var kbpgp = Promise.promisifyAll(require("kbpgp"));

var KeybaseSignin = module.exports.KeybaseSignin = function (params) {
    if (params.AWS) {
        this.resultCallback = KeybaseSignin.makeAWSLambdaCallback(params.AWS);
    } else if (params.Response) {
        this.resultCallback = KeybaseSignin.makeHTTPCallback(params.Response);
    }
};

KeybaseSignin.pkey_username_url = "https://keybase.io:443/_/api/1.0/user/lookup.json?usernames=%s&fields=basics,profile,public_keys";
KeybaseSignin.pkey_fingerprint_url = "https://keybase.io:443/_/api/1.0/user/lookup.json?key_fingerprint=%s&fields=basics,profile,public_keys";

KeybaseSignin.validateBlob = function (blob) {
	return blob.siteId && blob.token && blob.token.length >= 85;
};

KeybaseSignin.validateSignature = function (blob, blobFromSignature) {
    var keys = [
        "siteId",
        "token",
        "email_or_username",
        "fingerprint",
        "kb_login_ext_nonce",
        "kb_login_ext_annotation"
    ];

    for (var i = 0; i < keys.length; i++) {
        var k = keys[i];
        if (blob[k] !== blobFromSignature[k]) {
            return false;
        }
    }
    return true;
};

KeybaseSignin.validatePublicData = function(publicData) {
    var user = null;
    if (publicData &&
           publicData.status &&
           publicData.status.name === "OK" &&
           publicData.them &&
           publicData.them.length &&
           publicData.them[0].public_keys &&
           publicData.them[0].public_keys.primary &&
           publicData.them[0].public_keys.primary.bundle) {
        user = publicData.them[0];
    }
    return user;
};

KeybaseSignin.makeAWSLambdaCallback = function(params) {
    var AWS = require('aws-sdk');

    var identityPoolId = params.IdentityPoolId;
    var loginProvider = params.LoginProvider;
    var identityId = params.IdentityId;
    var context = params.LambdaContext;

    return function(errorCode, result) {
        var kb_uid;
        if (errorCode == 200 && result && result.user && result.user.kb_uid) {
            var params = {
                IdentityPoolId: identityPoolId,
                IdentityId: identityId,
                Logins: {}
            };
            params.Logins[loginProvider] = result.user.kb_uid;
            var cognito = new AWS.CognitoIdentity();
            cognito.getOpenIdTokenForDeveloperIdentity(params, function(err, data) {
                if (err) {
                    console.log(err, err.stack);
                    context.fail("Unable to obtain AWS credentials.");
                } else {
                    result.user.identity = data;
                    context.succeed(result);
                }
            });
        } else {
            context.fail(result);
        }
    };
};

KeybaseSignin.makeHTTPCallback = function(params) {
    var resp = params.ResponseObject;
    var successCb = params.SuccessCallback;
    var errorCb = params.ErrorCallback;

	return function (code, stringOrObj) {
		resp.status(code).send(stringOrObj);
		if (code == 200 && successCb) {
			successCb(stringOrObj);
		} else if (errorCb) {
            errorCb(stringOrObj);
        }
	};
};

KeybaseSignin.prototype.lookupKeybase = function (blob, signature) {
    var kb = this;

	if (!KeybaseSignin.validateBlob(blob)) {
		console.log("Signature blob not valid. Blob: " + blob);
		this.resultCallback(400, "Invalid signature blob");
	}

	var lookupCallback = function (response) {
		var body = '';

		response.on('data', function (chunk) {
			body += chunk;
		});

		response.on('end', function () {
            var publicData = JSON.parse(body);
            kb.handleKbCertVerify(publicData, blob, signature);
		});
	};

	var lookupUrl;
	if (blob.fingerprint) {
		lookupUrl = util.format(KeybaseSignin.pkey_fingerprint_url, blob.fingerprint);
	} else {
		lookupUrl = util.format(KeybaseSignin.pkey_username_url, blob.email_or_username);
	}
	https.get(lookupUrl, lookupCallback);
};

KeybaseSignin.prototype.handleKbCertVerify = async(function(publicData, blob, signature) {
    try {
        var user = KeybaseSignin.validatePublicData(publicData);
        if (!user) {
            throw "Error obtaining matching public key";
        }
        var kms, km, ring, literals;
        try {
            kms = await(kbpgp.KeyManager.import_from_armored_pgpAsync({armored: user.public_keys.primary.bundle}));
        } catch (err) {
            throw "Unable to load public key";
        }
        if (!kms) {
            throw "Unable to load key manager";
        }
        km = kms[0];
        ring = new kbpgp.keyring.KeyRing;
        ring.add_key_managerAsync(km);
        try {
            literals = await(kbpgp.unboxAsync({keyfetch: ring, armored: signature}));
        } catch(err) {
            throw "Unable to verify signature";
        }
        var decryptedSignature = literals[0].toString();
        var blobFromSignature = JSON.parse(decryptedSignature);
        if (!KeybaseSignin.validateSignature(blob, blobFromSignature)) {
            throw "Mismatched blob and signature";
        }
        var user_name = "",
            location = "";
        if (user['profile']) {
            var profile = user['profile'];
            user_name = profile['full_name'] || user_name;
            location = profile['location'] || location;
        }
        this.resultCallback(200, {
            status: {code: 0, name: "OK"},
            user: {
                kb_username: user['basics']['username'],
                kb_uid: user['id'],
                full_name: user_name,
                location: location,
                token: blob.token
            }
        });
    } catch (err) {
        console.log("Error: " + err);
        this.resultCallback(400, err);
    }
});
