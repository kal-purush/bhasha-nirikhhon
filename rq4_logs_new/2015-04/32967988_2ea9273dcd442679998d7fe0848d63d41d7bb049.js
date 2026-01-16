/* Copyright 2013 Sfeir Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Client ID of the application (from the APIs Console).
 *
 * @type {string}
 */
//Below is for the deployed
//CLIENT_ID = '767636924532-a69g5elglcgv4rk8a9d64ng41jrvcuns.apps.googleusercontent.com';
//Below is for localhost testing
CLIENT_ID = '767636924532-43i4f4p4nsuqhabgfbkufd3jiia172ba.apps.googleusercontent.com';
/**
 * Scopes used by the application.
 *
 * @type {string}
 */
SCOPES = 'https://www.googleapis.com/auth/userinfo.email';

/**
 * Response type of the auth token.
 *
 * @type {string}
 */
RESPONSE_TYPE = 'token id_token';

/**
 * Initializes the application. It loads asynchronously all needed libraries
 *
 * @param {string}
 *            apiRoot Root of the API's path.
 */
init = function() {
	var apisToLoad;
  var apiRoot = '//' + window.location.host + '/_ah/api';
	var callback = function() {
		if (--apisToLoad == 0) {
			//bootstrap manually angularjs after our api are loaded
			angular.bootstrap(document, [ "App" ]);
		}
	}
	apisToLoad = 2; // must match number of calls to gapi.client.load()
	gapi.client.load('hangman', 'v1', callback, apiRoot);
	gapi.client.load('oauth2', 'v2', callback);
};