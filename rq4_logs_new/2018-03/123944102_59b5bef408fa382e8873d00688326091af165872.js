/******/ (function(modules) { // webpackBootstrap
/******/ 	// The module cache
/******/ 	var installedModules = {};

/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {

/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId])
/******/ 			return installedModules[moduleId].exports;

/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			exports: {},
/******/ 			id: moduleId,
/******/ 			loaded: false
/******/ 		};

/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);

/******/ 		// Flag the module as loaded
/******/ 		module.loaded = true;

/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}


/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;

/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;

/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "";

/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(0);
/******/ })
/************************************************************************/
/******/ ([
/* 0 */
/***/ (function(module, exports, __webpack_require__) {

	var cdbConfig; // = require('./cdb.config.js');
	var DocumentClient = __webpack_require__(1).DocumentClient

	var client;

	var collectionLink;


	//From json-merge package
	var isJSON = function(json){

		let jsonC = {}.constructor ;

		if(json && json.constructor === jsonC){
			return true ;
		}else{
			return false ;
		}
	} ;

	var cloneJSON = function(data){
		return mergeJSON({}, data) ;
	} ;

	var mergeJSON = function(json1, json2){
		var result = null ;
		if(isJSON(json2)){
			result = {} ;
			if(isJSON(json1)){
				for(var key in json1){
					if(isJSON(json1[key]) || Array.isArray(json1[key])) {
						result[key] = cloneJSON(json1[key]) ;
					} else {
						result[key] = json1[key];
					}
				}
			}

			for(var key in json2){
				if(isJSON(json2[key]) || Array.isArray(json2[key])){
					result[key] = mergeJSON(result[key], json2[key]) ;
				}else{
					result[key] = json2[key] ;
				}
			}
		}else if(Array.isArray(json1) && Array.isArray(json2)){
			result = json1 ;

			for(var i = 0; i < json2.length; i++){
				if(result.indexOf(json2[i]) === -1){
					result[result.length] = json2[i] ;
				}
			}
		}else{
			result = json2 ;
		}

		return result ;
	} ;


	function initClient(reqCollection, callback){

		if (!client) {

			if (!cdbConfig) throw new Error('Please specify configuration parameters');

			let collection = (!reqCollection) ?  ((!cdbConfig.defaultCollection) ? ()=>{throw new Error('No default collection configured')} : cdbConfig.defaultCollection) : reqCollection;

			if (!cdbConfig.endpoint) throw new Error('No database endpoint configured');
			if (!cdbConfig.primaryKey) throw new Error('No database key configured');
			if (!cdbConfig.database) throw new Error('No database name configured');

			//format for collectionURL https://{databaseaccount}.documents.azure.com/dbs/{db}/colls/{coll}

			collectionLink = `dbs/${cdbConfig.database}/colls/${collection}/`
			

			//console.log("collectionLink: "+collectionLink);

			//let host = ;

			//console.log("host: "+host)

			client = new DocumentClient (cdbConfig.endpoint, {masterKey: cdbConfig.primaryKey})

			callback (null, "Client initialised");
		} else {
			callback (null, "Client exists");
		};
	}

	function insertDoc(payload, callback){

		//console.log(collectionLink);
		//console.log(payload);

		console.log("Start: "+Date.now())

		client.createDocument(collectionLink, payload, function (err, document) {
	        if (err) {
	            console.log(err);
	            callback(err);
	        } else {
	            console.log('created ' + document.id + ' at ' + Date.now());
	            callback(null,document.id);
	        }
	    });
	}

	function updateDoc(updatePayload, callback) {

		let docId = updatePayload.id

		let documentURI = collectionLink + 'docs/' + docId 

		client.readDocument(documentURI, (err,result)=>{
			if (err) {
				console.log("Unable to read document")
				callback(err)
			} else {

				let returnPayload = mergeJSON(result, updatePayload);

				client.replaceDocument(documentURI, returnPayload, function (err, document) {
			        if (err) {
			            console.log(err);
			            callback(err);
			        } else {
			            console.log('Updated ' + document.id);
			            callback(null,document.id);
			        }
				})
			}
		})
	}


	function replaceDoc(payload, callback){

		let docId = payload.id

		let documentURI = collectionLink + 'docs/' + docId 

		client.replaceDocument(documentURI, payload, function (err, document) {
	        if (err) {
	            console.log(err);
	            callback(err);
	        } else {
	            console.log('Replaced ' + document.id);
	            callback(null,document.id);
	        }
	    });
	}

	function queryDoc(query, callback){

		client.queryDocuments(collectionLink, query).toArray((err, result)=> {
	        if (err) {
	            console.log(err);
	            callback(err);
	        } else {

	            callback(null,result);
	        }
	    });
	}

	module.exports = {
		setConfig: function (config) {
			if (config) {
				cdbConfig = config;
			} else {
				throw new Error('Config not specified');
			}
		},
		insert: function (collection, payload, callback){
					if (!callback) {
						callback=payload;
						payload=collection;
					}

					if (!payload) {throw new Error('No data to be written')};

					initClient(collection, (err,result)=>
					{
						if (!err) {
							insertDoc(payload, (err,result)=>{
								if (err) {
									callback(err);
								} else {
									callback(null,result)
								}
							})

						} else {
							console.log ("Error Initialising Client: "+err);
							callback(err);
						}		
					})
				},

		insertBulk: function(collection, payload, callback){

						const each = __webpack_require__(142);

						if (!payload) {throw new Error('No data to be written')};

						initClient(collection, (err,result)=>
						{
							if (!err) {
								each(payload,insertDoc, (err,result)=>{
									if (err) {
										callback (err)
									} else {
										callback (null, result);
									}
								})
							} else {
								console.log ("Error Initialising Client: "+err);
								callback(err);
							}		
						})
					}, 
		update: function (collection, payload, callback){
					if (!payload) {throw new Error('No data to be written')};

					initClient(collection, (err,result)=>
					{
						if (!err) {

							if (!payload.id) {throw new Error('No documentId specified. Unable to update.')};

							updateDoc(payload,(err,result)=>{
								if (err) {
									callback(err);
								} else {
									callback(null,result)
								}
							})

						} else {
							console.log ("Error Initialising Client: "+err);
							callback(err);
						}		
					})
				},
		replace: function (collection, payload, callback){
					if (!payload) {throw new Error('No data to be written')};

					initClient(collection, (err,result)=>
					{
						if (!err) {

							if (!payload.id) {throw new Error('No documentId specified. Unable to update.')};

							replaceDoc(payload,(err,result)=>{
								if (err) {
									callback(err);
								} else {
									callback(null,result)
								}
							})

						} else {
							console.log ("Error Initialising Client: "+err);
							callback(err);
						}		
					})
				},
		updateBulk:	function (collection, payloadArray, callback){

					if (!payload) {throw new Error('No data to be written')};

					const each = __webpack_require__(142);

					initClient(collection, (err,result)=>
					{
						if (!err) {
							each(payloadArray,updateDoc, (err,result)=>{
								if (err) {
									callback (err)
								} else {
									callback (null, result);
								}
							})
						} else {
							console.log ("Error Initialising Client: "+err);
							callback(err);
						}		
					})
				},
		query: function(collection, query, callback){
					if (!query) {throw new Error('No query specified')};

					initClient(collection, (err,result)=>
					{
						if (!err) {
							queryDoc(query, (err,result)=>{
								if (err) {
									callback (err)
								} else {
									callback (null, result);
									console.log("Query Result: ");
									let i=0;
									let len = result.length;
									
									for (i=0;i<len;i++){
										console.log(result[i])
									}
								}
							})
						} else {
							console.log ("Error Initialising Client: "+err)
							callback(err);
						}		
					})
				}
		};

/***/ }),
/* 1 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2014 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	module.exports = __webpack_require__(2);

/***/ }),
/* 2 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Client = __webpack_require__(3)
	  , Hash = __webpack_require__(137)
	  , Range = __webpack_require__(140)
	  , UriFactory = __webpack_require__(141);

	if (true) {
	    exports.DocumentClient = Client.DocumentClient;
	    exports.DocumentBase = Client.DocumentBase;
	    exports.Base = Client.Base;
	    exports.Constants = Client.Constants;
	    exports.RetryOptions = Client.RetryOptions;
	    exports.Range = Range.Range;
	    exports.RangePartitionResolver = Range.RangePartitionResolver;
	    exports.HashPartitionResolver = Hash.HashPartitionResolver;
	    exports.UriFactory = UriFactory.UriFactory;
	}

/***/ }),
/* 3 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , https = __webpack_require__(73)
	    , url = __webpack_require__(94)
	    , tunnel = __webpack_require__(101)
	    , AzureDocuments = __webpack_require__(104)
	    , QueryIterator = __webpack_require__(106)
	    , RequestHandler = __webpack_require__(126)
	    , RetryOptions = __webpack_require__(105)
	    , GlobalEndpointManager = __webpack_require__(131)
	    , Constants = __webpack_require__(69)
	    , Helper = __webpack_require__(132).Helper
	    , util = __webpack_require__(17)
	    , Platform = __webpack_require__(70)
	    , SessionContainer = __webpack_require__(133);

	//SCRIPT START
	var DocumentClient = Base.defineClass(
	    /**
	     * Provides a client-side logical representation of the Azure Cosmos DB database account.
	     * This client is used to configure and execute requests in the Azure Cosmos DB database service.
	     * @constructor DocumentClient
	     * @param {string} urlConnection           - The service endpoint to use to create the client.
	     * @param {object} auth                    - An object that is used for authenticating requests and must contains one of the options
	     * @param {string} [auth.masterKey]        - The authorization master key to use to create the client.
	     * @param {Object} [auth.resourceTokens]   - An object that contains resources tokens. Keys for the object are resource Ids and values are the resource tokens.
	     * @param {Array}  [auth.permissionFeed]   - An array of {@link Permission} objects.
	     * @param {object} [connectionPolicy]      - An instance of {@link ConnectionPolicy} class. This parameter is optional and the default connectionPolicy will be used if omitted.
	     * @param {string} [consistencyLevel]      - An optional parameter that represents the consistency level. It can take any value from {@link ConsistencyLevel}.
	    */
	    function DocumentClient(urlConnection, auth, connectionPolicy, consistencyLevel) {
	        this.urlConnection = urlConnection;
	        if (auth !== undefined) {
	            this.masterKey = auth.masterKey;
	            this.resourceTokens = auth.resourceTokens;
	            if (auth.permissionFeed) {
	                this.resourceTokens = {};
	                for (var i = 0; i < auth.permissionFeed.length; i++) {
	                    var resourceId = Helper.getResourceIdFromPath(auth.permissionFeed[i].resource);
	                    if (!resourceId) {
	                        throw new Error("authorization error: " + resourceId + "is an invalid resourceId in permissionFeed");
	                    }

	                    this.resourceTokens[resourceId] = auth.permissionFeed[i]._token;
	                }
	            }
	        }

	        this.connectionPolicy = connectionPolicy || new AzureDocuments.ConnectionPolicy();
	        this.consistencyLevel = consistencyLevel;
	        this.defaultHeaders = {};
	        this.defaultHeaders[Constants.HttpHeaders.CacheControl] = "no-cache";
	        this.defaultHeaders[Constants.HttpHeaders.Version] = Constants.CurrentVersion;
	        if (consistencyLevel !== undefined) {
	            this.defaultHeaders[Constants.HttpHeaders.ConsistencyLevel] = consistencyLevel;
	        }

	        var platformDefaultHeaders = Platform.getPlatformDefaultHeaders() || {};
	        for (var platformDefaultHeader in platformDefaultHeaders) {
	            this.defaultHeaders[platformDefaultHeader] = platformDefaultHeaders[platformDefaultHeader];
	        }

	        this.defaultHeaders[Constants.HttpHeaders.UserAgent] = Platform.getUserAgent();

	        // overide this for default query params to be added to the url.
	        this.defaultUrlParams = "";

	        // Query compatibility mode.
	        // Allows to specify compatibility mode used by client when making query requests. Should be removed when
	        // application/sql is no longer supported.
	        this.queryCompatibilityMode = AzureDocuments.QueryCompatibilityMode.Default;
	        this.partitionResolvers = {};

	        this.partitionKeyDefinitionCache = {};

	        this._globalEndpointManager = new GlobalEndpointManager(this);

	        this.sessionContainer = new SessionContainer(this.urlConnection);

	        // Initialize request agent
	        var requestAgentOptions = { keepAlive: true, maxSockets: Infinity };
	        if (!!this.connectionPolicy.ProxyUrl) {
	            var proxyUrl = url.parse(this.connectionPolicy.ProxyUrl);
	            requestAgentOptions.proxy = {
	                host: proxyUrl.hostname,
	                port: proxyUrl.port
	            };

	            if (!!proxyUrl.auth) {
	                requestAgentOptions.proxy.proxyAuth = proxyUrl.auth;
	            }

	            this.requestAgent = proxyUrl.protocol.toLowerCase() === "https:" ?
	                tunnel.httpsOverHttps(requestAgentOptions) :
	                tunnel.httpsOverHttp(requestAgentOptions);
	        } else {
	            this.requestAgent = new https.Agent(requestAgentOptions);
	        }
	    },
	    {
	        /** Gets the curent write endpoint for a geo-replicated database account.
	         * @memberof DocumentClient
	         * @instance
	         * @param {function} callback        - The callback function which takes endpoint(string) as an argument.
	        */
	        getWriteEndpoint: function (callback) {
	            this._globalEndpointManager.getWriteEndpoint(function (writeEndpoint) {
	                callback(writeEndpoint);
	            });
	        },

	        /** Gets the curent read endpoint for a geo-replicated database account.
	         * @memberof DocumentClient
	         * @instance
	         * @param {function} callback        - The callback function which takes endpoint(string) as an argument.
	        */
	        getReadEndpoint: function (callback) {
	            this._globalEndpointManager.getReadEndpoint(function (readEndpoint) {
	                callback(readEndpoint);
	            });
	        },

	        /** Send a request for creating a database.
	         * <p>
	         *  A database manages users, permissions and a set of collections.  <br>
	         *  Each Azure Cosmos DB Database Account is able to support multiple independent named databases, with the database being the logical container for data. <br>
	         *  Each Database consists of one or more collections, each of which in turn contain one or more documents. Since databases are an an administrative resource, the Service Master Key will be required in order to access and successfully complete any action using the User APIs. <br>
	         * </p>
	         * @memberof DocumentClient
	         * @instance
	         * @param {Object} body              - A json object that represents The database to be created.
	         * @param {string} body.id           - The id of the database.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        createDatabase: function (body, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var err = {};
	            if (!this.isResourceValid(body, err)) {
	                callback(err);
	                return;
	            }

	            var path = "/dbs";
	            this.create(body, path, "dbs", undefined, undefined, options, callback);
	        },

	        /**
	         * Creates a collection.
	         * <p>
	         * A collection is a named logical container for documents. <br>
	         * A database may contain zero or more named collections and each collection consists of zero or more JSON documents. <br>
	         * Being schema-free, the documents in a collection do not need to share the same structure or fields. <br>
	         * Since collections are application resources, they can be authorized using either the master key or resource keys. <br>
	         * </p>
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} databaseLink                  - The self-link of the database.
	         * @param {object} body                          - Represents the body of the collection.
	         * @param {string} body.id                       - The id of the collection.
	         * @param {IndexingPolicy} body.indexingPolicy   - The indexing policy associated with the collection.
	         * @param {number} body.defaultTtl               - The default time to live in seconds for documents in a collection.
	         * @param {RequestOptions} [options]             - The request options.
	         * @param {RequestCallback} callback             - The callback for the request.
	         */
	        createCollection: function (databaseLink, body, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var err = {};
	            if (!this.isResourceValid(body, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(databaseLink);
	            var path = this.getPathFromLink(databaseLink, "colls", isNameBased);
	            var id = this.getIdFromLink(databaseLink, isNameBased);

	            this.create(body, path, "colls", id, undefined, options, callback);
	        },

	        /**
	         * Create a document.
	         * <p>
	         * There is no set schema for JSON documents. They may contain any number of custom properties as well as an optional list of attachments. <br>
	         * A Document is an application resource and can be authorized using the master key or resource keys
	         * </p>
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} documentsFeedOrDatabaseLink               - The collection link or database link if using a partition resolver
	         * @param {object} body                                      - Represents the body of the document. Can contain any number of user defined properties.
	         * @param {string} [body.id]                                 - The id of the document, MUST be unique for each document.
	         * @param {number} body.ttl                                  - The time to live in seconds of the document.
	         * @param {RequestOptions} [options]                         - The request options.
	         * @param {boolean} [options.disableAutomaticIdGeneration]   - Disables the automatic id generation. If id is missing in the body and this option is true, an error will be returned.
	         * @param {RequestCallback} callback                         - The callback for the request.
	         */
	        createDocument: function (documentsFeedOrDatabaseLink, body, options, callback) {
	            var partitionResolver = this.partitionResolvers[documentsFeedOrDatabaseLink];

	            var collectionLink;
	            if (partitionResolver === undefined || partitionResolver === null) {
	                collectionLink = documentsFeedOrDatabaseLink;
	            } else {
	                collectionLink = this.resolveCollectionLinkForCreate(partitionResolver, body);
	            }

	            this.createDocumentPrivate(collectionLink, body, options, callback);
	        },

	        /**
	         * Create an attachment for the document object.
	         * <p>
	         * Each document may contain zero or more attachments. Attachments can be of any MIME type - text, image, binary data. <br>
	         * These are stored externally in Azure Blob storage. Attachments are automatically deleted when the parent document is deleted.
	         * </P>
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} documentLink         - The self-link of the document.
	         * @param {Object} body                 - The metadata the defines the attachment media like media, contentType. It can include any other properties as part of the metedata.
	         * @param {string} body.contentType     - The MIME contentType of the attachment.
	         * @param {string} body.media           - Media link associated with the attachment content.
	         * @param {RequestOptions} options      - The request options.
	         * @param {RequestCallback} callback    - The callback for the request.
	        */
	        createAttachment: function (documentLink, body, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var err = {};
	            if (!this.isResourceValid(body, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(documentLink);
	            var path = this.getPathFromLink(documentLink, "attachments", isNameBased);
	            var id = this.getIdFromLink(documentLink, isNameBased);

	            this.create(body, path, "attachments", id, undefined, options, callback);
	        },

	        /**
	         * Create a database user.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} databaseLink         - The self-link of the database.
	         * @param {object} body                 - Represents the body of the user.
	         * @param {string} body.id              - The id of the user.
	         * @param {RequestOptions} [options]    - The request options.
	         * @param {RequestCallback} callback    - The callback for the request.
	         */
	        createUser: function (databaseLink, body, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var err = {};
	            if (!this.isResourceValid(body, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(databaseLink);
	            var path = this.getPathFromLink(databaseLink, "users", isNameBased);
	            var id = this.getIdFromLink(databaseLink, isNameBased);

	            this.create(body, path, "users", id, undefined, options, callback);
	        },

	        /**
	         * Create a permission.
	         * <p> A permission represents a per-User Permission to access a specific resource e.g. Document or Collection.  </p>
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} userLink             - The self-link of the user.
	         * @param {object} body                 - Represents the body of the permission.
	         * @param {string} body.id              - The id of the permission
	         * @param {string} body.permissionMode  - The mode of the permission, must be a value of {@link PermissionMode}
	         * @param {string} body.resource        - The link of the resource that the permission will be applied to.
	         * @param {RequestOptions} [options]    - The request options.
	         * @param {RequestCallback} callback    - The callback for the request.
	         */
	        createPermission: function (userLink, body, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var err = {};
	            if (!this.isResourceValid(body, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(userLink);
	            var path = this.getPathFromLink(userLink, "permissions", isNameBased);
	            var id = this.getIdFromLink(userLink, isNameBased);

	            this.create(body, path, "permissions", id, undefined, options, callback);
	        },

	        /**
	        * Create a trigger.
	        * <p>
	        * Azure Cosmos DB supports pre and post triggers defined in JavaScript to be executed on creates, updates and deletes. <br>
	        * For additional details, refer to the server-side JavaScript API documentation.
	        * </p>
	        * @memberof DocumentClient
	        * @instance
	        * @param {string} collectionLink           - The self-link of the collection.
	        * @param {object} trigger                  - Represents the body of the trigger.
	        * @param {string} trigger.id             - The id of the trigger.
	        * @param {string} trigger.triggerType      - The type of the trigger, should be one of the values of {@link TriggerType}.
	        * @param {string} trigger.triggerOperation - The trigger operation, should be one of the values of {@link TriggerOperation}.
	        * @param {function} trigger.serverScript   - The body of the trigger, it can be passed as stringified too.
	        * @param {RequestOptions} [options]        - The request options.
	        * @param {RequestCallback} callback        - The callback for the request.
	        */
	        createTrigger: function (collectionLink, trigger, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            if (trigger.serverScript) {
	                trigger.body = trigger.serverScript.toString();
	            } else if (trigger.body) {
	                trigger.body = trigger.body.toString();
	            }

	            var err = {};
	            if (!this.isResourceValid(trigger, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "triggers", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            this.create(trigger, path, "triggers", id, undefined, options, callback);
	        },

	        /**
	         * Create a UserDefinedFunction.
	         * <p>
	         * Azure Cosmos DB supports JavaScript UDFs which can be used inside queries, stored procedures and triggers. <br>
	         * For additional details, refer to the server-side JavaScript API documentation.
	         * </p>
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink                - The self-link of the collection.
	         * @param {object} udf                           - Represents the body of the userDefinedFunction.
	         * @param {string} udf.id                      - The id of the udf.
	         * @param {string} udf.userDefinedFunctionType   - The type of the udf, it should be one of the values of {@link UserDefinedFunctionType}
	         * @param {function} udf.serverScript            - Represents the body of the udf, it can be passed as stringified too.
	         * @param {RequestOptions} [options]             - The request options.
	         * @param {RequestCallback} callback             - The callback for the request.
	         */
	        createUserDefinedFunction: function (collectionLink, udf, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            if (udf.serverScript) {
	                udf.body = udf.serverScript.toString();
	            } else if (udf.body) {
	                udf.body = udf.body.toString();
	            }

	            var err = {};
	            if (!this.isResourceValid(udf, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "udfs", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            this.create(udf, path, "udfs", id, undefined, options, callback);
	        },

	        /**
	         * Create a StoredProcedure.
	         * <p>
	         * Azure Cosmos DB allows stored procedures to be executed in the storage tier, directly against a document collection. The script <br>
	         * gets executed under ACID transactions on the primary storage partition of the specified collection. For additional details, <br>
	         * refer to the server-side JavaScript API documentation.
	         * </p>
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink       - The self-link of the collection.
	         * @param {object} sproc                - Represents the body of the stored procedure.
	         * @param {string} sproc.id           - The id of the stored procedure.
	         * @param {function} sproc.serverScript - The body of the stored procedure, it can be passed as stringified too.
	         * @param {RequestOptions} [options]    - The request options.
	         * @param {RequestCallback} callback    - The callback for the request.
	         */
	        createStoredProcedure: function (collectionLink, sproc, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            if (sproc.serverScript) {
	                sproc.body = sproc.serverScript.toString();
	            } else if (sproc.body) {
	                sproc.body = sproc.body.toString();
	            }

	            var err = {};
	            if (!this.isResourceValid(sproc, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "sprocs", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            this.create(sproc, path, "sprocs", id, undefined, options, callback);
	        },

	        /**
	         * Create an attachment for the document object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} documentLink             - The self-link of the document.
	         * @param {stream.Readable} readableStream  - the stream that represents the media itself that needs to be uploaded.
	         * @param {MediaOptions} [options]          - The request options.
	         * @param {RequestCallback} callback        - The callback for the request.
	        */
	        createAttachmentAndUploadMedia: function (documentLink, readableStream, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var initialHeaders = Base.extend({}, this.defaultHeaders);
	            initialHeaders = Base.extend(initialHeaders, options && options.initialHeaders);

	            // Add required headers slug and content-type.
	            if (options.slug) {
	                initialHeaders[Constants.HttpHeaders.Slug] = options.slug;
	            }

	            if (options.contentType) {
	                initialHeaders[Constants.HttpHeaders.ContentType] = options.contentType;
	            } else {
	                initialHeaders[Constants.HttpHeaders.ContentType] = Constants.MediaTypes.OctetStream;
	            }

	            var isNameBased = Base.isLinkNameBased(documentLink);
	            var path = this.getPathFromLink(documentLink, "attachments", isNameBased);
	            var id = this.getIdFromLink(documentLink, isNameBased);

	            this.create(readableStream, path, "attachments", id, initialHeaders, options, callback);
	        },

	        /** Reads a database.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} databaseLink         - The self-link of the database.
	         * @param {RequestOptions} [options]    - The request options.
	         * @param {RequestCallback} callback    - The callback for the request.
	        */
	        readDatabase: function (databaseLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(databaseLink);
	            var path = this.getPathFromLink(databaseLink, "", isNameBased);
	            var id = this.getIdFromLink(databaseLink, isNameBased);

	            this.read(path, "dbs", id, undefined, options, callback);
	        },

	        /**
	         * Reads a collection.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink       - The self-link of the collection.
	         * @param {RequestOptions} [options]    - The request options.
	         * @param {RequestCallback} callback    - The callback for the request.
	         */
	        readCollection: function (collectionLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            var that = this;
	            this.read(path, "colls", id, undefined, options, function (err, collection, headers) {
	                if (err) return callback(err, collection, headers);
	                that.partitionKeyDefinitionCache[collectionLink] = collection.partitionKey;
	                callback(err, collection, headers);
	            });
	        },

	        /**
	         * Reads a document.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} documentLink         - The self-link of the document.
	         * @param {RequestOptions} [options]    - The request options.
	         * @param {RequestCallback} callback    - The callback for the request.
	         */
	        readDocument: function (documentLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(documentLink);
	            var path = this.getPathFromLink(documentLink, "", isNameBased);
	            var id = this.getIdFromLink(documentLink, isNameBased);

	            this.read(path, "docs", id, undefined, options, callback);
	        },

	        /**
	         * Reads an Attachment object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} attachmentLink    - The self-link of the attachment.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        readAttachment: function (attachmentLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(attachmentLink);
	            var path = this.getPathFromLink(attachmentLink, "", isNameBased);
	            var id = this.getIdFromLink(attachmentLink, isNameBased);

	            this.read(path, "attachments", id, undefined, options, callback);
	        },

	        /**
	         * Reads a user.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} userLink          - The self-link of the user.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	         */
	        readUser: function (userLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(userLink);
	            var path = this.getPathFromLink(userLink, "", isNameBased);
	            var id = this.getIdFromLink(userLink, isNameBased);

	            this.read(path, "users", id, undefined, options, callback);
	        },

	        /**
	         * Reads a permission.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} permissionLink    - The self-link of the permission.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	         */
	        readPermission: function (permissionLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(permissionLink);
	            var path = this.getPathFromLink(permissionLink, "", isNameBased);
	            var id = this.getIdFromLink(permissionLink, isNameBased);

	            this.read(path, "permissions", id, undefined, options, callback);
	        },

	        /**
	         * Reads a trigger object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} triggerLink       - The self-link of the trigger.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	         */
	        readTrigger: function (triggerLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var resourceInfo = Base.parseLink(triggerLink);

	            var isNameBased = Base.isLinkNameBased(triggerLink);
	            var path = this.getPathFromLink(triggerLink, "", isNameBased);
	            var id = this.getIdFromLink(triggerLink, isNameBased);

	            this.read(path, "triggers", id, undefined, options, callback);
	        },

	        /**
	         * Reads a udf object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} udfLink           - The self-link of the user defined function.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	         */
	        readUserDefinedFunction: function (udfLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(udfLink);
	            var path = this.getPathFromLink(udfLink, "", isNameBased);
	            var id = this.getIdFromLink(udfLink, isNameBased);

	            this.read(path, "udfs", id, undefined, options, callback);
	        },

	        /**
	         * Reads a StoredProcedure object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} sprocLink         - The self-link of the stored procedure.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	         */
	        readStoredProcedure: function (sprocLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(sprocLink);
	            var path = this.getPathFromLink(sprocLink, "", isNameBased);
	            var id = this.getIdFromLink(sprocLink, isNameBased);

	            this.read(path, "sprocs", id, undefined, options, callback);
	        },

	        /**
	         * Reads a conflict.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} conflictLink      - The self-link of the conflict.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	         */
	        readConflict: function (conflictLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(conflictLink);
	            var path = this.getPathFromLink(conflictLink, "", isNameBased);
	            var id = this.getIdFromLink(conflictLink, isNameBased);

	            this.read(path, "conflicts", id, undefined, options, callback);
	        },

	        /** Lists all databases.
	         * @memberof DocumentClient
	         * @instance
	         * @param {FeedOptions} [options] - The feed options.
	         * @returns {QueryIterator}       - An instance of queryIterator to handle reading feed.
	        */
	        readDatabases: function (options) {
	            return this.queryDatabases(undefined, options);
	        },

	        /**
	         * Get all collections in this database.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} databaseLink   - The self-link of the database.
	         * @param {FeedOptions} [options] - The feed options.
	         * @returns {QueryIterator}       - An instance of queryIterator to handle reading feed.
	         */
	        readCollections: function (databaseLink, options) {
	            return this.queryCollections(databaseLink, undefined, options);
	        },

	        /**
	         * Get all documents in this collection.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink - The self-link of the collection.
	         * @param {FeedOptions} [options] - The feed options.
	         * @returns {QueryIterator}       - An instance of queryIterator to handle reading feed.
	         */
	        readDocuments: function (collectionLink, options) {
	            return this.queryDocuments(collectionLink, undefined, options);
	        },

	        /**
	         * Get all Partition key Ranges in this collection.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink - The self-link of the collection.
	         * @param {FeedOptions} [options] - The feed options.
	         * @returns {QueryIterator}       - An instance of queryIterator to handle reading feed.
	         * @ignore
	         */
	        readPartitionKeyRanges: function (collectionLink, options) {
	            return this.queryPartitionKeyRanges(collectionLink, undefined, options);
	        },

	        /**
	        * Get all attachments for this document.
	        * @memberof DocumentClient
	        * @instance
	        * @param {string} documentLink   - The self-link of the document.
	        * @param {FeedOptions} [options] - The feed options.
	        * @returns {QueryIterator}       - An instance of queryIterator to handle reading feed.
	       */
	        readAttachments: function (documentLink, options) {
	            return this.queryAttachments(documentLink, undefined, options);
	        },

	        /**
	         * Get all users in this database.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} databaseLink       - The self-link of the database.
	         * @param {FeedOptions} [feedOptions] - The feed options.
	         * @returns {QueryIterator}           - An instance of queryIterator to handle reading feed.
	         */
	        readUsers: function (databaseLink, options) {
	            return this.queryUsers(databaseLink, undefined, options);
	        },

	        /**
	         * Get all permissions for this user.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} userLink           - The self-link of the user.
	         * @param {FeedOptions} [feedOptions] - The feed options.
	         * @returns {QueryIterator}           - An instance of queryIterator to handle reading feed.
	         */
	        readPermissions: function (userLink, options) {
	            return this.queryPermissions(userLink, undefined, options);
	        },

	        /**
	         * Get all triggers in this collection.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink   - The self-link of the collection.
	         * @param {FeedOptions} [options]   - The feed options.
	         * @returns {QueryIterator}         - An instance of queryIterator to handle reading feed.
	         */
	        readTriggers: function (collectionLink, options) {
	            return this.queryTriggers(collectionLink, undefined, options);
	        },

	        /**
	         * Get all UserDefinedFunctions in this collection.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink - The self-link of the collection.
	         * @param {FeedOptions} [options] - The feed options.
	         * @returns {QueryIterator}       - An instance of queryIterator to handle reading feed.
	         */
	        readUserDefinedFunctions: function (collectionLink, options) {
	            return this.queryUserDefinedFunctions(collectionLink, undefined, options);
	        },

	        /**
	         * Get all StoredProcedures in this collection.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink - The self-link of the collection.
	         * @param {FeedOptions} [options] - The feed options.
	         * @returns {QueryIterator}       - An instance of queryIterator to handle reading feed.
	         */
	        readStoredProcedures: function (collectionLink, options) {
	            return this.queryStoredProcedures(collectionLink, undefined, options);
	        },

	        /**
	         * Get all conflicts in this collection.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink - The self-link of the collection.
	         * @param {FeedOptions} [options] - The feed options.
	         * @returns {QueryIterator}       - An instance of QueryIterator to handle reading feed.
	         */
	        readConflicts: function (collectionLink, options) {
	            return this.queryConflicts(collectionLink, undefined, options);
	        },

	        /** Lists all databases that satisfy a query.
	         * @memberof DocumentClient
	         * @instance
	         * @param {SqlQuerySpec | string} query - A SQL query.
	         * @param {FeedOptions} [options]       - The feed options.
	         * @returns {QueryIterator}             - An instance of QueryIterator to handle reading feed.
	        */
	        queryDatabases: function (query, options) {
	            var that = this;
	            return new QueryIterator(this, query, options, function (options, callback) {
	                that.queryFeed.call(that,
	                    that,
	                    "/dbs",
	                    "dbs",
	                    "",
	                    function (result) { return result.Databases; },
	                    function (parent, body) { return body; },
	                    query,
	                    options,
	                    callback);
	            });
	        },

	        /**
	         * Query the collections for the database.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} databaseLink           - The self-link of the database.
	         * @param {SqlQuerySpec | string} query   - A SQL query.
	         * @param {FeedOptions} [options]         - Represents the feed options.
	         * @returns {QueryIterator}               - An instance of queryIterator to handle reading feed.
	         */
	        queryCollections: function (databaseLink, query, options) {
	            var that = this;

	            var isNameBased = Base.isLinkNameBased(databaseLink);
	            var path = this.getPathFromLink(databaseLink, "colls", isNameBased);
	            var id = this.getIdFromLink(databaseLink, isNameBased);

	            return new QueryIterator(this, query, options, function (options, callback) {
	                that.queryFeed.call(that,
	                    that,
	                    path,
	                    "colls",
	                    id,
	                    function (result) { return result.DocumentCollections; },
	                    function (parent, body) { return body; },
	                    query,
	                    options,
	                    callback);
	            });
	        },

	        /**
	         * Query the documents for the collection.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} documentsFeedOrDatabaseLink          - The collection link or database link if using a partition resolver
	         * @param {SqlQuerySpec | string} query                 - A SQL query.
	         * @param {FeedOptions} [options]                       - Represents the feed options.
	         * @param {object} [options.partitionKey]               - Optional partition key to be used with the partition resolver
	         * @returns {QueryIterator}                             - An instance of queryIterator to handle reading feed.
	         */
	        queryDocuments: function (documentsFeedOrDatabaseLink, query, options) {
	            var partitionResolver = this.partitionResolvers[documentsFeedOrDatabaseLink];
	            var collectionLinks;
	            if (partitionResolver === undefined || partitionResolver === null) {
	                collectionLinks = [documentsFeedOrDatabaseLink];
	            } else {
	                collectionLinks = partitionResolver.resolveForRead(options && options.partitionKey);
	            }

	            return this.queryDocumentsPrivate(collectionLinks, query, options);
	        },

	        /**
	         * Query the partition key ranges
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} databaseLink           - The self-link of the database.
	         * @param {SqlQuerySpec | string} query   - A SQL query.
	         * @param {FeedOptions} [options]         - Represents the feed options.
	         * @returns {QueryIterator}               - An instance of queryIterator to handle reading feed.
	         * @ignore
	         */
	        queryPartitionKeyRanges: function (collectionLink, query, options) {
	            var that = this;

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "pkranges", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            return new QueryIterator(this, query, options, function (options, callback) {
	                that.queryFeed.call(that,
	                    that,
	                    path,
	                    "pkranges",
	                    id,
	                    function (result) { return result.PartitionKeyRanges; },
	                    function (parent, body) { return body; },
	                    query,
	                    options,
	                    callback);
	            });
	        },


	        /**
	         * Query the attachments for the document.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} documentLink           - The self-link of the document.
	         * @param {SqlQuerySpec | string} query   - A SQL query.
	         * @param {FeedOptions} [options]         - Represents the feed options.
	         * @returns {QueryIterator}               - An instance of queryIterator to handle reading feed.
	        */
	        queryAttachments: function (documentLink, query, options) {
	            var that = this;

	            var isNameBased = Base.isLinkNameBased(documentLink);
	            var path = this.getPathFromLink(documentLink, "attachments", isNameBased);
	            var id = this.getIdFromLink(documentLink, isNameBased);

	            return new QueryIterator(this, query, options, function (options, callback) {
	                that.queryFeed.call(that,
	                    that,
	                    path,
	                    "attachments",
	                    id,
	                    function (result) { return result.Attachments; },
	                    function (parent, body) { return body; },
	                    query,
	                    options,
	                    callback);
	            });
	        },

	        /**
	         * Query the users for the database.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} databaseLink           - The self-link of the database.
	         * @param {SqlQuerySpec | string} query   - A SQL query.
	         * @param {FeedOptions} [options]         - Represents the feed options.
	         * @returns {QueryIterator}               - An instance of queryIterator to handle reading feed.
	         */
	        queryUsers: function (databaseLink, query, options) {
	            var that = this;

	            var isNameBased = Base.isLinkNameBased(databaseLink);
	            var path = this.getPathFromLink(databaseLink, "users", isNameBased);
	            var id = this.getIdFromLink(databaseLink, isNameBased);

	            return new QueryIterator(this, query, options, function (options, callback) {
	                that.queryFeed.call(that,
	                    that,
	                    path,
	                    "users",
	                    id,
	                    function (result) { return result.Users; },
	                    function (parent, body) { return body; },
	                    query,
	                    options,
	                    callback);
	            });
	        },

	        /**
	         * Query the permission for the user.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} userLink               - The self-link of the user.
	         * @param {SqlQuerySpec | string} query   - A SQL query.
	         * @param {FeedOptions} [options]         - Represents the feed options.
	         * @returns {QueryIterator}               - An instance of queryIterator to handle reading feed.
	         */
	        queryPermissions: function (userLink, query, options) {
	            var that = this;

	            var isNameBased = Base.isLinkNameBased(userLink);
	            var path = this.getPathFromLink(userLink, "permissions", isNameBased);
	            var id = this.getIdFromLink(userLink, isNameBased);

	            return new QueryIterator(this, query, options, function (options, callback) {
	                that.queryFeed.call(that,
	                    that,
	                    path,
	                    "permissions",
	                    id,
	                    function (result) { return result.Permissions; },
	                    function (parent, body) { return body; },
	                    query,
	                    options,
	                    callback);
	            });
	        },

	        /**
	         * Query the triggers for the collection.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink         - The self-link of the collection.
	         * @param {SqlQuerySpec | string} query   - A SQL query.
	         * @param {FeedOptions} [options]         - Represents the feed options.
	         * @returns {QueryIterator}               - An instance of queryIterator to handle reading feed.
	         */
	        queryTriggers: function (collectionLink, query, options) {
	            var that = this;

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "triggers", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            return new QueryIterator(this, query, options, function (options, callback) {
	                that.queryFeed.call(that,
	                    that,
	                    path,
	                    "triggers",
	                    id,
	                    function (result) { return result.Triggers; },
	                    function (parent, body) { return body; },
	                    query,
	                    options,
	                    callback);
	            });
	        },

	        /**
	         * Query the user defined functions for the collection.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink         - The self-link of the collection.
	         * @param {SqlQuerySpec | string} query   - A SQL query.
	         * @param {FeedOptions} [options]         - Represents the feed options.
	         * @returns {QueryIterator}               - An instance of queryIterator to handle reading feed.
	         */
	        queryUserDefinedFunctions: function (collectionLink, query, options) {
	            var that = this;

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "udfs", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            return new QueryIterator(this, query, options, function (options, callback) {
	                that.queryFeed.call(that,
	                    that,
	                    path,
	                    "udfs",
	                    id,
	                    function (result) { return result.UserDefinedFunctions; },
	                    function (parent, body) { return body; },
	                    query,
	                    options,
	                    callback);
	            });
	        },

	        /**
	         * Query the storedProcedures for the collection.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink         - The self-link of the collection.
	         * @param {SqlQuerySpec | string} query   - A SQL query.
	         * @param {FeedOptions} [options]         - Represents the feed options.
	         * @returns {QueryIterator}               - An instance of queryIterator to handle reading feed.
	         */
	        queryStoredProcedures: function (collectionLink, query, options) {
	            var that = this;

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "sprocs", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            return new QueryIterator(this, query, options, function (options, callback) {
	                that.queryFeed.call(that,
	                    that,
	                    path,
	                    "sprocs",
	                    id,
	                    function (result) { return result.StoredProcedures; },
	                    function (parent, body) { return body; },
	                    query,
	                    options,
	                    callback);
	            });
	        },

	        /**
	         * Query the conflicts for the collection.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink         - The self-link of the collection.
	         * @param {SqlQuerySpec | string} query   - A SQL query.
	         * @param {FeedOptions} [options]         - Represents the feed options.
	         * @returns {QueryIterator}               - An instance of queryIterator to handle reading feed.
	         */
	        queryConflicts: function (collectionLink, query, options) {
	            var that = this;

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "conflicts", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            return new QueryIterator(this, query, options, function (options, callback) {
	                that.queryFeed.call(that,
	                    that,
	                    path,
	                    "conflicts",
	                    id,
	                    function (result) { return result.Conflicts; },
	                    function (parent, body) { return body; },
	                    query,
	                    options,
	                    callback);
	            });
	        },

	        /**
	         * Delete the database object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} databaseLink         - The self-link of the database.
	         * @param {RequestOptions} [options]    - The request options.
	         * @param {RequestCallback} callback    - The callback for the request.
	        */
	        deleteDatabase: function (databaseLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(databaseLink);
	            var path = this.getPathFromLink(databaseLink, "", isNameBased);
	            var id = this.getIdFromLink(databaseLink, isNameBased);
	            this.deleteResource(path, "dbs", id, undefined, options, callback);
	        },

	        /**
	         * Delete the collection object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink    - The self-link of the collection.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        deleteCollection: function (collectionLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            this.deleteResource(path, "colls", id, undefined, options, callback);
	        },

	        /**
	         * Delete the document object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} documentLink      - The self-link of the document.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        deleteDocument: function (documentLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(documentLink);
	            var path = this.getPathFromLink(documentLink, "", isNameBased);
	            var id = this.getIdFromLink(documentLink, isNameBased);

	            this.deleteResource(path, "docs", id, undefined, options, callback);
	        },

	        /**
	         * Delete the attachment object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} attachmentLink    - The self-link of the attachment.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	         */
	        deleteAttachment: function (attachmentLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(attachmentLink);
	            var path = this.getPathFromLink(attachmentLink, "", isNameBased);
	            var id = this.getIdFromLink(attachmentLink, isNameBased);

	            this.deleteResource(path, "attachments", id, undefined, options, callback);
	        },

	        /**
	         * Delete the user object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} userLink          - The self-link of the user.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        deleteUser: function (userLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(userLink);
	            var path = this.getPathFromLink(userLink, "", isNameBased);
	            var id = this.getIdFromLink(userLink, isNameBased);

	            this.deleteResource(path, "users", id, undefined, options, callback);
	        },

	        /**
	         * Delete the permission object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} permissionLink    - The self-link of the permission.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        deletePermission: function (permissionLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(permissionLink);
	            var path = this.getPathFromLink(permissionLink, "", isNameBased);
	            var id = this.getIdFromLink(permissionLink, isNameBased);

	            this.deleteResource(path, "permissions", id, undefined, options, callback);
	        },

	        /**
	         * Delete the trigger object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} triggerLink       - The self-link of the trigger.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        deleteTrigger: function (triggerLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(triggerLink);
	            var path = this.getPathFromLink(triggerLink, "", isNameBased);
	            var id = this.getIdFromLink(triggerLink, isNameBased);

	            this.deleteResource(path, "triggers", id, undefined, options, callback);
	        },

	        /**
	         * Delete the UserDefinedFunction object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} udfLink           - The self-link of the user defined function.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        deleteUserDefinedFunction: function (udfLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(udfLink);
	            var path = this.getPathFromLink(udfLink, "", isNameBased);
	            var id = this.getIdFromLink(udfLink, isNameBased);

	            this.deleteResource(path, "udfs", id, undefined, options, callback);
	        },

	        /**
	         * Delete the StoredProcedure object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} sprocLink         - The self-link of the stored procedure.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        deleteStoredProcedure: function (sprocLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(sprocLink);
	            var path = this.getPathFromLink(sprocLink, "", isNameBased);
	            var id = this.getIdFromLink(sprocLink, isNameBased);

	            this.deleteResource(path, "sprocs", id, undefined, options, callback);
	        },

	        /**
	         * Delete the conflict object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} conflictLink      - The self-link of the conflict.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        deleteConflict: function (conflictLink, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var isNameBased = Base.isLinkNameBased(conflictLink);
	            var path = this.getPathFromLink(conflictLink, "", isNameBased);
	            var id = this.getIdFromLink(conflictLink, isNameBased);

	            this.deleteResource(path, "conflicts", id, undefined, options, callback);
	        },

	        /**
	         * Replace the document collection.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink    - The self-link of the document collection.
	         * @param {object} collection        - Represent the new document collection body.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        replaceCollection: function (collectionLink, collection, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var err = {};
	            if (!this.isResourceValid(collection, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            this.replace(collection, path, "colls", id, undefined, options, callback);
	        },

	        /**
	         * Replace the document object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} documentLink      - The self-link of the document.
	         * @param {object} document          - Represent the new document body.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        replaceDocument: function (documentLink, newDocument, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var that = this;

	            var task = function () {
	                var err = {};
	                if (!that.isResourceValid(newDocument, err)) {
	                    callback(err);
	                    return;
	                }

	                var isNameBased = Base.isLinkNameBased(documentLink);
	                var path = that.getPathFromLink(documentLink, "", isNameBased);
	                var id = that.getIdFromLink(documentLink, isNameBased);

	                that.replace(newDocument, path, "docs", id, undefined, options, callback);
	            };

	            if (options.partitionKey === undefined && options.skipGetPartitionKeyDefinition !== true) {
	                this.getPartitionKeyDefinition(Base.getCollectionLink(documentLink), function (err, partitionKeyDefinition, response, headers) {
	                    if (err) return callback(err, response, headers);
	                    options.partitionKey = that.extractPartitionKey(newDocument, partitionKeyDefinition);

	                    task();
	                });
	            }
	            else {
	                task();
	            }
	        },

	        /**
	         * Replace the attachment object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} attachmentLink    - The self-link of the attachment.
	         * @param {object} attachment        - Represent the new attachment body.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	         */
	        replaceAttachment: function (attachmentLink, attachment, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var err = {};
	            if (!this.isResourceValid(attachment, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(attachmentLink);
	            var path = this.getPathFromLink(attachmentLink, "", isNameBased);
	            var id = this.getIdFromLink(attachmentLink, isNameBased);

	            this.replace(attachment, path, "attachments", id, undefined, options, callback);
	        },

	        /**
	         * Replace the user object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} userLink          - The self-link of the user.
	         * @param {object} user              - Represent the new user body.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        replaceUser: function (userLink, user, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var err = {};
	            if (!this.isResourceValid(user, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(userLink);
	            var path = this.getPathFromLink(userLink, "", isNameBased);
	            var id = this.getIdFromLink(userLink, isNameBased);

	            this.replace(user, path, "users", id, undefined, options, callback);
	        },

	        /**
	         * Replace the permission object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} permissionLink    - The self-link of the permission.
	         * @param {object} permission        - Represent the new permission body.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        replacePermission: function (permissionLink, permission, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var err = {};
	            if (!this.isResourceValid(permission, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(permissionLink);
	            var path = this.getPathFromLink(permissionLink, "", isNameBased);
	            var id = this.getIdFromLink(permissionLink, isNameBased);

	            this.replace(permission, path, "permissions", id, undefined, options, callback);
	        },

	        /**
	         * Replace the trigger object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} triggerLink       - The self-link of the trigger.
	         * @param {object} trigger           - Represent the new trigger body.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        replaceTrigger: function (triggerLink, trigger, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            if (trigger.serverScript) {
	                trigger.body = trigger.serverScript.toString();
	            } else if (trigger.body) {
	                trigger.body = trigger.body.toString();
	            }

	            var err = {};
	            if (!this.isResourceValid(trigger, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(triggerLink);
	            var path = this.getPathFromLink(triggerLink, "", isNameBased);
	            var id = this.getIdFromLink(triggerLink, isNameBased);

	            this.replace(trigger, path, "triggers", id, undefined, options, callback);
	        },

	        /**
	         * Replace the UserDefinedFunction object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} udfLink           - The self-link of the user defined function.
	         * @param {object} udf               - Represent the new udf body.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        replaceUserDefinedFunction: function (udfLink, udf, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            if (udf.serverScript) {
	                udf.body = udf.serverScript.toString();
	            } else if (udf.body) {
	                udf.body = udf.body.toString();
	            }

	            var err = {};
	            if (!this.isResourceValid(udf, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(udfLink);
	            var path = this.getPathFromLink(udfLink, "", isNameBased);
	            var id = this.getIdFromLink(udfLink, isNameBased);

	            this.replace(udf, path, "udfs", id, undefined, options, callback);
	        },

	        /**
	         * Replace the StoredProcedure object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} sprocLink         - The self-link of the stored procedure.
	         * @param {object} sproc             - Represent the new sproc body.
	         * @param {RequestOptions} [options] - The request options.
	         * @param {RequestCallback} callback - The callback for the request.
	        */
	        replaceStoredProcedure: function (sprocLink, sproc, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            if (sproc.serverScript) {
	                sproc.body = sproc.serverScript.toString();
	            } else if (sproc.body) {
	                sproc.body = sproc.body.toString();
	            }

	            var err = {};
	            if (!this.isResourceValid(sproc, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(sprocLink);
	            var path = this.getPathFromLink(sprocLink, "", isNameBased);
	            var id = this.getIdFromLink(sprocLink, isNameBased);

	            this.replace(sproc, path, "sprocs", id, undefined, options, callback);
	        },

	        /**
	         * Upsert a document.
	         * <p>
	         * There is no set schema for JSON documents. They may contain any number of custom properties as well as an optional list of attachments. <br>
	         * A Document is an application resource and can be authorized using the master key or resource keys
	         * </p>
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} documentsFeedOrDatabaseLink               - The collection link or database link if using a partition resolver
	         * @param {object} body                                      - Represents the body of the document. Can contain any number of user defined properties.
	         * @param {string} [body.id]                                 - The id of the document, MUST be unique for each document.
	         * @param {number} body.ttl                                  - The time to live in seconds of the document.
	         * @param {RequestOptions} [options]                         - The request options.
	         * @param {boolean} [options.disableAutomaticIdGeneration]   - Disables the automatic id generation. If id is missing in the body and this option is true, an error will be returned.
	         * @param {RequestCallback} callback                         - The callback for the request.
	         */
	        upsertDocument: function (documentsFeedOrDatabaseLink, body, options, callback) {
	            var partitionResolver = this.partitionResolvers[documentsFeedOrDatabaseLink];

	            var collectionLink;
	            if (partitionResolver === undefined || partitionResolver === null) {
	                collectionLink = documentsFeedOrDatabaseLink;
	            } else {
	                collectionLink = this.resolveCollectionLinkForCreate(partitionResolver, body);
	            }

	            this.upsertDocumentPrivate(collectionLink, body, options, callback);
	        },

	        /**
	         * Upsert an attachment for the document object.
	         * <p>
	         * Each document may contain zero or more attachments. Attachments can be of any MIME type - text, image, binary data. <br>
	         * These are stored externally in Azure Blob storage. Attachments are automatically deleted when the parent document is deleted.
	         * </P>
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} documentLink         - The self-link of the document.
	         * @param {Object} body                 - The metadata the defines the attachment media like media, contentType. It can include any other properties as part of the metedata.
	         * @param {string} body.contentType     - The MIME contentType of the attachment.
	         * @param {string} body.media           - Media link associated with the attachment content.
	         * @param {RequestOptions} options      - The request options.
	         * @param {RequestCallback} callback    - The callback for the request.
	        */
	        upsertAttachment: function (documentLink, body, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var err = {};
	            if (!this.isResourceValid(body, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(documentLink);
	            var path = this.getPathFromLink(documentLink, "attachments", isNameBased);
	            var id = this.getIdFromLink(documentLink, isNameBased);

	            this.upsert(body, path, "attachments", id, undefined, options, callback);
	        },

	        /**
	         * Upsert a database user.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} databaseLink         - The self-link of the database.
	         * @param {object} body                 - Represents the body of the user.
	         * @param {string} body.id              - The id of the user.
	         * @param {RequestOptions} [options]    - The request options.
	         * @param {RequestCallback} callback    - The callback for the request.
	         */
	        upsertUser: function (databaseLink, body, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var err = {};
	            if (!this.isResourceValid(body, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(databaseLink);
	            var path = this.getPathFromLink(databaseLink, "users", isNameBased);
	            var id = this.getIdFromLink(databaseLink, isNameBased);

	            this.upsert(body, path, "users", id, undefined, options, callback);
	        },

	        /**
	         * Upsert a permission.
	         * <p> A permission represents a per-User Permission to access a specific resource e.g. Document or Collection.  </p>
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} userLink             - The self-link of the user.
	         * @param {object} body                 - Represents the body of the permission.
	         * @param {string} body.id              - The id of the permission
	         * @param {string} body.permissionMode  - The mode of the permission, must be a value of {@link PermissionMode}
	         * @param {string} body.resource        - The link of the resource that the permission will be applied to.
	         * @param {RequestOptions} [options]    - The request options.
	         * @param {RequestCallback} callback    - The callback for the request.
	         */
	        upsertPermission: function (userLink, body, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var err = {};
	            if (!this.isResourceValid(body, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(userLink);
	            var path = this.getPathFromLink(userLink, "permissions", isNameBased);
	            var id = this.getIdFromLink(userLink, isNameBased);

	            this.upsert(body, path, "permissions", id, undefined, options, callback);
	        },

	        /**
	        * Upsert a trigger.
	        * <p>
	        * Azure Cosmos DB supports pre and post triggers defined in JavaScript to be executed on creates, updates and deletes. <br>
	        * For additional details, refer to the server-side JavaScript API documentation.
	        * </p>
	        * @memberof DocumentClient
	        * @instance
	        * @param {string} collectionLink           - The self-link of the collection.
	        * @param {object} trigger                  - Represents the body of the trigger.
	        * @param {string} trigger.id             - The id of the trigger.
	        * @param {string} trigger.triggerType      - The type of the trigger, should be one of the values of {@link TriggerType}.
	        * @param {string} trigger.triggerOperation - The trigger operation, should be one of the values of {@link TriggerOperation}.
	        * @param {function} trigger.serverScript   - The body of the trigger, it can be passed as stringified too.
	        * @param {RequestOptions} [options]        - The request options.
	        * @param {RequestCallback} callback        - The callback for the request.
	        */
	        upsertTrigger: function (collectionLink, trigger, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            if (trigger.serverScript) {
	                trigger.body = trigger.serverScript.toString();
	            } else if (trigger.body) {
	                trigger.body = trigger.body.toString();
	            }

	            var err = {};
	            if (!this.isResourceValid(trigger, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "triggers", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            this.upsert(trigger, path, "triggers", id, undefined, options, callback);
	        },

	        /**
	         * Upsert a UserDefinedFunction.
	         * <p>
	         * Azure Cosmos DB supports JavaScript UDFs which can be used inside queries, stored procedures and triggers. <br>
	         * For additional details, refer to the server-side JavaScript API documentation.
	         * </p>
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink                - The self-link of the collection.
	         * @param {object} udf                           - Represents the body of the userDefinedFunction.
	         * @param {string} udf.id                      - The id of the udf.
	         * @param {string} udf.userDefinedFunctionType   - The type of the udf, it should be one of the values of {@link UserDefinedFunctionType}
	         * @param {function} udf.serverScript            - Represents the body of the udf, it can be passed as stringified too.
	         * @param {RequestOptions} [options]             - The request options.
	         * @param {RequestCallback} callback             - The callback for the request.
	         */
	        upsertUserDefinedFunction: function (collectionLink, udf, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            if (udf.serverScript) {
	                udf.body = udf.serverScript.toString();
	            } else if (udf.body) {
	                udf.body = udf.body.toString();
	            }

	            var err = {};
	            if (!this.isResourceValid(udf, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "udfs", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            this.upsert(udf, path, "udfs", id, undefined, options, callback);
	        },

	        /**
	         * Upsert a StoredProcedure.
	         * <p>
	         * Azure Cosmos DB allows stored procedures to be executed in the storage tier, directly against a document collection. The script <br>
	         * gets executed under ACID transactions on the primary storage partition of the specified collection. For additional details, <br>
	         * refer to the server-side JavaScript API documentation.
	         * </p>
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} collectionLink       - The self-link of the collection.
	         * @param {object} sproc                - Represents the body of the stored procedure.
	         * @param {string} sproc.id           - The id of the stored procedure.
	         * @param {function} sproc.serverScript - The body of the stored procedure, it can be passed as stringified too.
	         * @param {RequestOptions} [options]    - The request options.
	         * @param {RequestCallback} callback    - The callback for the request.
	         */
	        upsertStoredProcedure: function (collectionLink, sproc, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            if (sproc.serverScript) {
	                sproc.body = sproc.serverScript.toString();
	            } else if (sproc.body) {
	                sproc.body = sproc.body.toString();
	            }

	            var err = {};
	            if (!this.isResourceValid(sproc, err)) {
	                callback(err);
	                return;
	            }

	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var path = this.getPathFromLink(collectionLink, "sprocs", isNameBased);
	            var id = this.getIdFromLink(collectionLink, isNameBased);

	            this.upsert(sproc, path, "sprocs", id, undefined, options, callback);
	        },

	        /**
	         * Upsert an attachment for the document object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} documentLink             - The self-link of the document.
	         * @param {stream.Readable} readableStream  - the stream that represents the media itself that needs to be uploaded.
	         * @param {MediaOptions} [options]          - The request options.
	         * @param {RequestCallback} callback        - The callback for the request.
	        */
	        upsertAttachmentAndUploadMedia: function (documentLink, readableStream, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var initialHeaders = Base.extend({}, this.defaultHeaders);
	            initialHeaders = Base.extend(initialHeaders, options && options.initialHeaders);

	            // Add required headers slug and content-type.
	            if (options.slug) {
	                initialHeaders[Constants.HttpHeaders.Slug] = options.slug;
	            }

	            if (options.contentType) {
	                initialHeaders[Constants.HttpHeaders.ContentType] = options.contentType;
	            } else {
	                initialHeaders[Constants.HttpHeaders.ContentType] = Constants.MediaTypes.OctetStream;
	            }

	            var isNameBased = Base.isLinkNameBased(documentLink);
	            var path = this.getPathFromLink(documentLink, "attachments", isNameBased);
	            var id = this.getIdFromLink(documentLink, isNameBased);

	            this.upsert(readableStream, path, "attachments", id, initialHeaders, options, callback);
	        },

	        /**
	          * Read the media for the attachment object.
	          * @memberof DocumentClient
	          * @instance
	          * @param {string} mediaLink         - The media link of the media in the attachment.
	          * @param {RequestCallback} callback - The callback for the request, the result parameter can be a buffer or a stream
	          *                                     depending on the value of {@link MediaReadMode}.
	          */
	        readMedia: function (mediaLink, callback) {
	            var resourceInfo = Base.parseLink(mediaLink);
	            var path = "/" + mediaLink;
	            var initialHeaders = Base.extend({}, this.defaultHeaders);
	            initialHeaders[Constants.HttpHeaders.Accept] = Constants.MediaTypes.Any;
	            var attachmentId = Base.getAttachmentIdFromMediaId(resourceInfo.objectBody.id).toLowerCase();

	            var headers = Base.getHeaders(this, initialHeaders, "get", path, attachmentId, "media", {});

	            var that = this;
	            // readMedia will always use WriteEndpoint since it's not replicated in readable Geo regions
	            this._globalEndpointManager.getWriteEndpoint(function (writeEndpoint) {
	                that.get(writeEndpoint, path, headers, callback);
	            });
	        },

	        /**
	         * Update media for the attachment
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} mediaLink                - The media link of the media in the attachment.
	         * @param {stream.Readable} readableStream  - The stream that represents the media itself that needs to be uploaded.
	         * @param {MediaOptions} [options]          - options for the media
	         * @param {RequestCallback} callback        - The callback for the request.
	         */
	        updateMedia: function (mediaLink, readableStream, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var defaultHeaders = this.defaultHeaders;
	            var initialHeaders = Base.extend({}, defaultHeaders);
	            initialHeaders = Base.extend(initialHeaders, options && options.initialHeaders);

	            // Add required headers slug and content-type in case the body is a stream
	            if (options.slug) {
	                initialHeaders[Constants.HttpHeaders.Slug] = options.slug;
	            }

	            if (options.contentType) {
	                initialHeaders[Constants.HttpHeaders.ContentType] = options.contentType;
	            } else {
	                initialHeaders[Constants.HttpHeaders.ContentType] = Constants.MediaTypes.OctetStream;
	            }

	            initialHeaders[Constants.HttpHeaders.Accept] = Constants.MediaTypes.Any;

	            var resourceInfo = Base.parseLink(mediaLink);
	            var path = "/" + mediaLink;
	            var attachmentId = Base.getAttachmentIdFromMediaId(resourceInfo.objectBody.id).toLowerCase();
	            var headers = Base.getHeaders(this, initialHeaders, "put", path, attachmentId, "media", options);

	            // updateMedia will use WriteEndpoint since it uses PUT operation
	            var that = this;
	            this._globalEndpointManager.getWriteEndpoint(function (writeEndpoint) {
	                that.put(writeEndpoint, path, readableStream, headers, callback);
	            });
	        },

	        /**
	         * Execute the StoredProcedure represented by the object with partition key.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} sprocLink            - The self-link of the stored procedure.
	         * @param {Array} [params]              - represent the parameters of the stored procedure.
	         * @param {Object} [options]            - partition key
	         * @param {RequestCallback} callback    - The callback for the request.
	        */
	        executeStoredProcedure: function (sprocLink, params, options, callback) {
	            if (!callback && !options) {
	                callback = params;
	                params = null;
	                options = {}
	            }
	            else if (!callback) {
	                callback = options;
	                options = {};
	            }

	            var defaultHeaders = this.defaultHeaders;
	            var initialHeaders = {};
	            initialHeaders = Base.extend(initialHeaders, defaultHeaders);
	            initialHeaders = Base.extend(initialHeaders, options && options.initialHeaders);

	            // Accept a single parameter or an array of parameters.
	            if (params !== null && params !== undefined && params.constructor !== Array) {
	                params = [params];
	            }

	            var isNameBased = Base.isLinkNameBased(sprocLink);
	            var path = this.getPathFromLink(sprocLink, "", isNameBased);
	            var id = this.getIdFromLink(sprocLink, isNameBased);

	            var headers = Base.getHeaders(this, initialHeaders, "post", path, id, "sprocs", options);

	            // executeStoredProcedure will use WriteEndpoint since it uses POST operation
	            var that = this;
	            this._globalEndpointManager.getWriteEndpoint(function (writeEndpoint) {
	                that.post(writeEndpoint, path, params, headers, callback);
	            });
	        },

	        /**
	         * Replace the offer object.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} offerLink         - The self-link of the offer.
	         * @param {object} offer             - Represent the new offer body.
	         * @param {RequestCallback} callback - The callback for the request.
	         */
	        replaceOffer: function (offerLink, offer, callback) {
	            var err = {};
	            if (!this.isResourceValid(offer, err)) {
	                callback(err);
	                return;
	            }

	            var path = "/" + offerLink;
	            var id = Base.parseLink(offerLink).objectBody.id.toLowerCase();
	            this.replace(offer, path, "offers", id, undefined, {}, callback);
	        },

	        /** Reads an offer.
	         * @memberof DocumentClient
	         * @instance
	         * @param {string} offerLink         - The self-link of the offer.
	         * @param {RequestCallback} callback    - The callback for the request.
	        */
	        readOffer: function (offerLink, callback) {
	            var path = "/" + offerLink;
	            var id = Base.parseLink(offerLink).objectBody.id.toLowerCase();
	            this.read(path, "offers", id, undefined, {}, callback);
	        },

	        /** Lists all offers.
	         * @memberof DocumentClient
	         * @instance
	         * @param {FeedOptions} [options] - The feed options.
	         * @returns {QueryIterator}       - An instance of queryIterator to handle reading feed.
	        */
	        readOffers: function (options) {
	            return this.queryOffers(undefined, options);
	        },

	        /** Lists all offers that satisfy a query.
	         * @memberof DocumentClient
	         * @instance
	         * @param {SqlQuerySpec | string} query - A SQL query.
	         * @param {FeedOptions} [options]       - The feed options.
	         * @returns {QueryIterator}             - An instance of QueryIterator to handle reading feed.
	        */
	        queryOffers: function (query, options) {
	            var that = this;
	            return new QueryIterator(this, query, options, function (options, callback) {
	                that.queryFeed.call(that,
	                    that,
	                    "/offers",
	                    "offers",
	                    "",
	                    function (result) { return result.Offers; },
	                    function (parent, body) { return body; },
	                    query,
	                    options,
	                    callback);
	            });
	        },

	        /** Gets the Database account information.
	       * @memberof DocumentClient
	       * @instance
	       * @param {string} [options.urlConnection]   - The endpoint url whose database account needs to be retrieved. If not present, current client's url will be used.
	       * @param {RequestCallback} callback         - The callback for the request. The second parameter of the callback will be of type {@link DatabaseAccount}.
	       */
	        getDatabaseAccount: function (options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var urlConnection = options.urlConnection || this.urlConnection;

	            var headers = Base.getHeaders(this, this.defaultHeaders, "get", "", "", "", {});
	            this.get(urlConnection, "", headers, function (err, result, headers) {
	                if (err) return callback(err);

	                var databaseAccount = new AzureDocuments.DatabaseAccount();
	                databaseAccount.DatabasesLink = "/dbs/";
	                databaseAccount.MediaLink = "/media/";
	                databaseAccount.MaxMediaStorageUsageInMB = headers[Constants.HttpHeaders.MaxMediaStorageUsageInMB];
	                databaseAccount.CurrentMediaStorageUsageInMB = headers[Constants.HttpHeaders.CurrentMediaStorageUsageInMB];
	                databaseAccount.ConsistencyPolicy = result.userConsistencyPolicy;

	                // WritableLocations and ReadableLocations properties will be available only for geo-replicated database accounts
	                if (Constants.WritableLocations in result) {
	                    databaseAccount._writableLocations = result[Constants.WritableLocations];
	                }
	                if (Constants.ReadableLocations in result) {
	                    databaseAccount._readableLocations = result[Constants.ReadableLocations];
	                }

	                callback(undefined, databaseAccount, headers);
	            });
	        },

	        /** @ignore */
	        createDocumentPrivate: function (collectionLink, body, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var that = this;

	            var task = function () {
	                // Generate random document id if the id is missing in the payload and options.disableAutomaticIdGeneration != true
	                if ((body.id === undefined || body.id === "") && !options.disableAutomaticIdGeneration) {
	                    body.id = Base.generateGuidId();
	                }

	                var err = {};
	                if (!that.isResourceValid(body, err)) {
	                    callback(err);
	                    return;
	                }

	                var isNameBased = Base.isLinkNameBased(collectionLink);
	                var path = that.getPathFromLink(collectionLink, "docs", isNameBased);
	                var id = that.getIdFromLink(collectionLink, isNameBased);

	                that.create(body, path, "docs", id, undefined, options, callback);
	            };

	            if (options.partitionKey === undefined && options.skipGetPartitionKeyDefinition !== true) {
	                this.getPartitionKeyDefinition(collectionLink, function (err, partitionKeyDefinition, response, headers) {
	                    if (err) return callback(err, response, headers);
	                    options.partitionKey = that.extractPartitionKey(body, partitionKeyDefinition);

	                    task();
	                });
	            }
	            else {
	                task();
	            }
	        },

	        /** @ignore */
	        upsertDocumentPrivate: function (collectionLink, body, options, callback) {
	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var that = this;

	            var task = function () {
	                // Generate random document id if the id is missing in the payload and options.disableAutomaticIdGeneration != true
	                if ((body.id === undefined || body.id === "") && !options.disableAutomaticIdGeneration) {
	                    body.id = Base.generateGuidId();
	                }

	                var err = {};
	                if (!that.isResourceValid(body, err)) {
	                    callback(err);
	                    return;
	                }

	                var isNameBased = Base.isLinkNameBased(collectionLink);
	                var path = that.getPathFromLink(collectionLink, "docs", isNameBased);
	                var id = that.getIdFromLink(collectionLink, isNameBased);

	                that.upsert(body, path, "docs", id, undefined, options, callback);
	            };

	            if (options.partitionKey === undefined && options.skipGetPartitionKeyDefinition !== true) {
	                this.getPartitionKeyDefinition(collectionLink, function (err, partitionKeyDefinition, response, headers) {
	                    if (err) return callback(err, response, headers);
	                    options.partitionKey = that.extractPartitionKey(body, partitionKeyDefinition);

	                    task();
	                });
	            }
	            else {
	                task();
	            }
	        },

	        /** @ignore */
	        queryDocumentsPrivate: function (collectionLinks, query, options) {
	            var that = this;

	            var fetchFunctions = Base.map(collectionLinks, function (collectionLink) {
	                var isNameBased = Base.isLinkNameBased(collectionLink);
	                var path = that.getPathFromLink(collectionLink, "docs", isNameBased);
	                var id = that.getIdFromLink(collectionLink, isNameBased);

	                return function (options, callback) {
	                    that.queryFeed.call(that,
	                        that,
	                        path,
	                        "docs",
	                        id,
	                        function (result) { return result.Documents; },
	                        function (parent, body) { return body; },
	                        query,
	                        options,
	                        callback);
	                };
	            });

	            return new QueryIterator(this, query, options, fetchFunctions, collectionLinks);
	        },

	        /** @ignore */
	        create: function (body, path, type, id, initialHeaders, options, callback) {
	            initialHeaders = initialHeaders || Base.extend({}, this.defaultHeaders);
	            initialHeaders = Base.extend(initialHeaders, options && options.initialHeaders);
	            var headers = Base.getHeaders(this, initialHeaders, "post", path, id, type, options);

	            var that = this;
	            this.applySessionToken(path, headers);

	            // create will use WriteEndpoint since it uses POST operation
	            this._globalEndpointManager.getWriteEndpoint(function (writeEndpoint) {
	                that.post(writeEndpoint, path, body, headers, function (err, result, resHeaders) {
	                    that.captureSessionToken(path, Constants.OperationTypes.Create, headers, resHeaders);
	                    callback(err, result, resHeaders);
	                });
	            });
	        },

	        /** @ignore */
	        upsert: function (body, path, type, id, initialHeaders, options, callback) {
	            initialHeaders = initialHeaders || Base.extend({}, this.defaultHeaders);
	            initialHeaders = Base.extend(initialHeaders, options && options.initialHeaders);
	            var headers = Base.getHeaders(this, initialHeaders, "post", path, id, type, options);
	            this.setIsUpsertHeader(headers);

	            var that = this;
	            this.applySessionToken(path, headers);

	            // upsert will use WriteEndpoint since it uses POST operation
	            this._globalEndpointManager.getWriteEndpoint(function (writeEndpoint) {
	                that.post(writeEndpoint, path, body, headers, function (err, result, resHeaders) {
	                    that.captureSessionToken(path, Constants.OperationTypes.Upsert, headers, resHeaders);
	                    callback(err, result, resHeaders);
	                });
	            });
	        },

	        /** @ignore */
	        replace: function (resource, path, type, id, initialHeaders, options, callback) {
	            initialHeaders = initialHeaders || Base.extend({}, this.defaultHeaders);
	            initialHeaders = Base.extend(initialHeaders, options && options.initialHeaders);
	            var headers = Base.getHeaders(this, initialHeaders, "put", path, id, type, options);

	            var that = this;
	            this.applySessionToken(path, headers);

	            // replace will use WriteEndpoint since it uses PUT operation
	            this._globalEndpointManager.getWriteEndpoint(function (writeEndpoint) {
	                that.put(writeEndpoint, path, resource, headers, function (err, result, resHeaders) {
	                    that.captureSessionToken(path, Constants.OperationTypes.Replace, headers, resHeaders);
	                    callback(err, result, resHeaders);
	                });
	            });
	        },

	        /** @ignore */
	        read: function (path, type, id, initialHeaders, options, callback) {
	            initialHeaders = initialHeaders || Base.extend({}, this.defaultHeaders);
	            initialHeaders = Base.extend(initialHeaders, options && options.initialHeaders);
	            var headers = Base.getHeaders(this, initialHeaders, "get", path, id, type, options);

	            var that = this;
	            this.applySessionToken(path, headers);

	            var request = { "path": path, "operationType": Constants.OperationTypes.Read, "client": this, "endpointOverride": null };

	            // read will use ReadEndpoint since it uses GET operation
	            this._globalEndpointManager.getReadEndpoint(function (readEndpoint) {
	                that.get(readEndpoint, request, headers, function (err, result, resHeaders) {
	                    that.captureSessionToken(path, Constants.OperationTypes.Read, headers, resHeaders);
	                    callback(err, result, resHeaders);
	                });
	            });
	        },

	        /** @ignore */
	        deleteResource: function (path, type, id, initialHeaders, options, callback) {
	            initialHeaders = initialHeaders || Base.extend({}, this.defaultHeaders);
	            initialHeaders = Base.extend(initialHeaders, options && options.initialHeaders);
	            var headers = Base.getHeaders(this, initialHeaders, "delete", path, id, type, options);

	            var that = this;
	            this.applySessionToken(path, headers);

	            // deleteResource will use WriteEndpoint since it uses DELETE operation
	            this._globalEndpointManager.getWriteEndpoint(function (writeEndpoint) {
	                that.delete(writeEndpoint, path, headers, function (err, result, resHeaders) {
	                    if (Base.parseLink(path).type != "colls")
	                        that.captureSessionToken(path, Constants.OperationTypes.Delete, headers, resHeaders);
	                    else
	                        that.clearSessionToken(path);
	                    callback(err, result, resHeaders);
	                });
	            });
	        },

	        /** @ignore */
	        get: function (url, request, headers, callback) {
	            return RequestHandler.request(this._globalEndpointManager, this.connectionPolicy, this.requestAgent, "GET", url, request, undefined, this.defaultUrlParams, headers, callback);
	        },

	        /** @ignore */
	        post: function (url, request, body, headers, callback) {
	            return RequestHandler.request(this._globalEndpointManager, this.connectionPolicy, this.requestAgent, "POST", url, request, body, this.defaultUrlParams, headers, callback);
	        },

	        /** @ignore */
	        put: function (url, request, body, headers, callback) {
	            return RequestHandler.request(this._globalEndpointManager, this.connectionPolicy, this.requestAgent, "PUT", url, request, body, this.defaultUrlParams, headers, callback);
	        },

	        /** @ignore */
	        head: function (url, request, headers, callback) {
	            return RequestHandler.request(this._globalEndpointManager, this.connectionPolicy, this.requestAgent, "HEAD", url, request, undefined, this.defaultUrlParams, headers, callback);
	        },

	        /** @ignore */
	        delete: function (url, request, headers, callback) {
	            return RequestHandler.request(this._globalEndpointManager, this.connectionPolicy, this.requestAgent, "DELETE", url, request, undefined, this.defaultUrlParams, headers, callback);
	        },

	        /** Gets the partition key definition first by looking into the cache otherwise by reading the collection.
	        * @ignore
	        * @param {string} collectionLink   - Link to the collection whose partition key needs to be extracted.
	        * @param {function} callback       - The arguments to the callback are(in order): error, partitionKeyDefinition, response object and response headers
	        */
	        getPartitionKeyDefinition: function (collectionLink, callback) {
	            // $ISSUE-felixfan-2016-03-17: Make name based path and link based path use the same key
	            // $ISSUE-felixfan-2016-03-17: Refresh partitionKeyDefinitionCache when necessary
	            if (collectionLink in this.partitionKeyDefinitionCache) {
	                return callback(undefined, this.partitionKeyDefinitionCache[collectionLink]);
	            }

	            var that = this;

	            this.readCollection(collectionLink, function (err, collection, headers) {
	                if (err) return callback(err, undefined, collection, headers);
	                callback(err, that.partitionKeyDefinitionCache[collectionLink], collection, headers);
	            });
	        },

	        extractPartitionKey: function (document, partitionKeyDefinition) {
	            if (partitionKeyDefinition && partitionKeyDefinition.paths && partitionKeyDefinition.paths.length > 0) {
	                var partitionKey = [];
	                partitionKeyDefinition.paths.forEach(function (path) {
	                    var pathParts = Base.parsePath(path);

	                    var obj = document;
	                    for (var i = 0; i < pathParts.length; ++i) {
	                        if (!((typeof obj === "object") && (pathParts[i] in obj))) {
	                            obj = {};
	                            break;
	                        }

	                        obj = obj[pathParts[i]];
	                    }

	                    partitionKey.push(obj);
	                });

	                return partitionKey;
	            }

	            return undefined;
	        },

	        /** @ignore */
	        queryFeed: function (documentclient, path, type, id, resultFn, createFn, query, options, callback, partitionKeyRangeId) {
	            var that = this;

	            var optionsCallbackTuple = this.validateOptionsAndCallback(options, callback);
	            options = optionsCallbackTuple.options;
	            callback = optionsCallbackTuple.callback;

	            var successCallback = function (err, result, responseHeaders) {
	                if (err) return callback(err, undefined, responseHeaders);
	                var bodies;
	                if (query) {
	                    bodies = resultFn(result);
	                }
	                else {
	                    bodies = Base.map(resultFn(result), function (body) {
	                        return createFn(that, body);
	                    });
	                }

	                callback(undefined, bodies, responseHeaders);
	            };

	            // Query operations will use ReadEndpoint even though it uses GET(for queryFeed) and POST(for regular query operations)
	            this._globalEndpointManager.getReadEndpoint(function (readEndpoint) {

	                var request = { "path": path, "operationType": Constants.OperationTypes.Query, "client": this, "endpointOverride": null };

	                var initialHeaders = Base.extend({}, documentclient.defaultHeaders);
	                initialHeaders = Base.extend(initialHeaders, options && options.initialHeaders);
	                if (query === undefined) {
	                    var headers = Base.getHeaders(documentclient, initialHeaders, "get", path, id, type, options, partitionKeyRangeId);
	                    that.applySessionToken(path, headers);

	                    documentclient.get(readEndpoint, request, headers, function (err, result, resHeaders) {
	                        that.captureSessionToken(path, Constants.OperationTypes.Query, headers, resHeaders);
	                        successCallback(err, result, resHeaders);
	                    });
	                } else {
	                    initialHeaders[Constants.HttpHeaders.IsQuery] = "true";
	                    switch (that.queryCompatibilityMode) {
	                        case AzureDocuments.QueryCompatibilityMode.SqlQuery:
	                            initialHeaders[Constants.HttpHeaders.ContentType] = Constants.MediaTypes.SQL;
	                            break;
	                        case AzureDocuments.QueryCompatibilityMode.Query:
	                        case AzureDocuments.QueryCompatibilityMode.Default:
	                        default:
	                            if (typeof query === "string") {
	                                query = { query: query };  // Converts query text to query object.
	                            }
	                            initialHeaders[Constants.HttpHeaders.ContentType] = Constants.MediaTypes.QueryJson;
	                            break;
	                    }

	                    var headers = Base.getHeaders(documentclient, initialHeaders, "post", path, id, type, options, partitionKeyRangeId);
	                    that.applySessionToken(path, headers);

	                    documentclient.post(readEndpoint, request, query, headers, function (err, result, resHeaders) {
	                        that.captureSessionToken(path, Constants.OperationTypes.Query, headers, resHeaders);
	                        successCallback(err, result, resHeaders);
	                    });
	                }
	            });
	        },

	        /** @ignore */
	        isResourceValid: function (resource, err) {
	            if (resource.id) {
	                if (typeof resource.id !== "string") {
	                    err.message = "Id must be a string.";
	                    return false;
	                }

	                if (resource.id.indexOf("/") !== -1 || resource.id.indexOf("\\") !== -1 || resource.id.indexOf("?") !== -1 || resource.id.indexOf("#") !== -1) {
	                    err.message = "Id contains illegal chars.";
	                    return false;
	                }
	                if (resource.id[resource.id.length - 1] === " ") {
	                    err.message = "Id ends with a space.";
	                    return false;
	                }
	            }
	            return true;
	        },

	        /** @ignore */
	        resolveCollectionLinkForCreate: function (partitionResolver, document) {
	            var validation = this.isPartitionResolverValid(partitionResolver);
	            if (!validation.valid) {
	                throw validation.error;
	            }

	            var partitionKey = partitionResolver.getPartitionKey(document);
	            return partitionResolver.resolveForCreate(partitionKey);
	        },

	        /** @ignore */
	        isPartitionResolverValid: function (partionResolver) {
	            if (partionResolver === null || partionResolver === undefined) {
	                return {
	                    valid: false,
	                    error: new Error("The partition resolver is null or undefined")
	                };
	            }

	            var validation = this.isPartitionResolveFunctionDefined(partionResolver, "getPartitionKey");
	            if (!validation.valid) {
	                return validation;
	            }
	            validation = this.isPartitionResolveFunctionDefined(partionResolver, "resolveForCreate");
	            if (!validation.valid) {
	                return validation;
	            }
	            validation = this.isPartitionResolveFunctionDefined(partionResolver, "resolveForRead");
	            return validation;
	        },

	        /** @ignore */
	        isPartitionResolveFunctionDefined: function (partionResolver, functionName) {
	            if (partionResolver === null || partionResolver === undefined) {
	                return {
	                    valid: false,
	                    error: new Error("The partition resolver is null or undefined")
	                };
	            }

	            if (typeof partionResolver[functionName] === "function") {
	                return {
	                    valid: true
	                };
	            } else {
	                return {
	                    valid: false,
	                    error: new Error(util.format("The partition resolver does not implement method %s. The type of %s is \"%s\"", functionName, functionName, typeof partionResolver[functionName]))
	                };
	            }
	        },

	        /** @ignore */
	        getIdFromLink: function (resourceLink, isNameBased) {
	            if (isNameBased) {
	                resourceLink = Base._trimSlashes(resourceLink);
	                return resourceLink;
	            } else {
	                return Base.parseLink(resourceLink).objectBody.id.toLowerCase();
	            }
	        },

	        /** @ignore */
	        getPathFromLink: function (resourceLink, resourceType, isNameBased) {
	            if (isNameBased) {
	                resourceLink = Base._trimSlashes(resourceLink);
	                if (resourceType) {
	                    return "/" + encodeURI(resourceLink) + "/" + resourceType;
	                } else {
	                    return "/" + encodeURI(resourceLink);
	                }
	            } else {
	                if (resourceType) {
	                    return "/" + resourceLink + resourceType + "/";
	                } else {
	                    return "/" + resourceLink;
	                }
	            }
	        },

	        /** @ignore */
	        setIsUpsertHeader: function (headers) {
	            if (headers === undefined || headers === null) {
	                throw new Error('The "headers" parameter must not be null or undefined');
	            }

	            if (!(headers instanceof Object)) {
	                throw new Error(util.format('The "headers" parameter must be an instance of "Object". Actual type is: "%s".', typeof headers));
	            }

	            headers[Constants.HttpHeaders.IsUpsert] = true;
	        },

	        /** @ignore */
	        validateOptionsAndCallback: function (optionsIn, callbackIn) {
	            var options, callback;

	            // options
	            if (optionsIn === undefined) {
	                options = new Object();
	            } else if (callbackIn === undefined && typeof optionsIn === 'function') {
	                callback = optionsIn;
	                options = new Object();
	            } else if (typeof optionsIn !== 'object') {
	                throw new Error(util.format('The "options" parameter must be of type "object". Actual type is: "%s".', typeof optionsIn));
	            } else {
	                options = optionsIn;
	            }

	            // callback
	            if (callbackIn !== undefined && typeof callbackIn !== 'function') {
	                throw new Error(util.format('The "callback" parameter must be of type "function". Actual type is: "%s".', typeof callbackIn));
	            } else if (typeof callbackIn === 'function') {
	                callback = callbackIn
	            }

	            return { options: options, callback: callback };
	        },

	        /** Gets the SessionToken for a given collectionLink
	         * @memberof DocumentClient
	         * @instance
	         * @param collectionLink              - The link of the collection for which the session token is needed 
	        */
	        getSessionToken: function (collectionLink) {
	            if (!collectionLink)
	                throw new Error("collectionLink cannot be null");

	            var paths = Base.parseLink(collectionLink);

	            if (paths == undefined)
	                return "";

	            var request = this.getSessionParams(collectionLink);
	            return this.sessionContainer.resolveGlobalSessionToken(request);
	        },

	        applySessionToken: function (path, reqHeaders) {
	            var request = this.getSessionParams(path);

	            if (reqHeaders && reqHeaders[Constants.HttpHeaders.SessionToken])
	                return;

	            var sessionConsistency = reqHeaders[Constants.HttpHeaders.ConsistencyLevel];
	            if (!sessionConsistency)
	                return;

	            if (request['resourceAddress']) {
	                var sessionToken = this.sessionContainer.resolveGlobalSessionToken(request);
	                if (sessionToken != "")
	                    reqHeaders[Constants.HttpHeaders.SessionToken] = sessionToken;
	            }
	        },

	        captureSessionToken: function (path, opType, reqHeaders, resHeaders) {
	            var request = this.getSessionParams(path);
	            request['operationType'] = opType;
	            this.sessionContainer.setSessionToken(request, reqHeaders, resHeaders);
	        },

	        clearSessionToken: function (path) {
	            var request = this.getSessionParams(path);
	            this.sessionContainer.clearToken(request);
	        },

	        getSessionParams: function (resourceLink) {
	            var isNameBased = Base.isLinkNameBased(resourceLink);
	            var resourceId = null;
	            var resourceAddress = null;
	            var parserOutput = Base.parseLink(resourceLink);
	            if (isNameBased)
	                resourceAddress = parserOutput.objectBody.self;
	            else {
	                resourceAddress = parserOutput.objectBody.id;
	                resourceId = parserOutput.objectBody.id;
	            }
	            var resourceType = parserOutput.type;
	            return { 'isNameBased': isNameBased, 'resourceId': resourceId, 'resourceAddress': resourceAddress, 'resourceType': resourceType };
	        }
	    }
	);
	//SCRIPT END

	/**
	 * The request options
	 * @typedef {Object} RequestOptions                          -         Options that can be specified for a requested issued to the Azure Cosmos DB servers.
	 * @property {object} [accessCondition]                      -         Conditions Associated with the request.
	 * @property {string} accessCondition.type                   -         Conditional HTTP method header type (IfMatch or IfNoneMatch).
	 * @property {string} accessCondition.condition              -         Conditional HTTP method header value (the _etag field from the last version you read).
	 * @property {string} [consistencyLevel]                     -         Consistency level required by the client.
	 * @property {boolean} [disableRUPerMinuteUsage]             -         DisableRUPerMinuteUsage is used to enable/disable Request Units(RUs)/minute capacity to serve the request if regular provisioned RUs/second is exhausted.
	 * @property {boolean} [enableScriptLogging]                 -         Enables or disables logging in JavaScript stored procedures.
	 * @property {string} [indexingDirective]                    -         Specifies indexing directives (index, do not index .. etc).
	 * @property {boolean} [offerEnableRUPerMinuteThroughput]    -         Represents Request Units(RU)/Minute throughput is enabled/disabled for a collection in the Azure Cosmos DB database service.
	 * @property {number} [offerThroughput]                      -         The offer throughput provisioned for a collection in measurement of Requests-per-Unit in the Azure Cosmos DB database service.
	 * @property {string} [offerType]                            -         Offer type when creating document collections.
	 *                                                                     <p>This option is only valid when creating a document collection.</p>
	 * @property {string} [partitionKey]                         -         Specifies a partition key definition for a particular path in the Azure Cosmos DB database service.
	 * @property {boolean} [populateQuotaInfo]                   -         Enables/disables getting document collection quota related stats for document collection read requests.
	 * @property {string} [postTriggerInclude]                   -         Indicates what is the post trigger to be invoked after the operation.
	 * @property {string} [preTriggerInclude]                    -         Indicates what is the pre trigger to be invoked before the operation.
	 * @property {number} [resourceTokenExpirySeconds]           -         Expiry time (in seconds) for resource token associated with permission (applicable only for requests on permissions).
	 * @property {string} [sessionToken]                         -         Token for use with Session consistency.
	 */

	/**
	 * The feed options
	 * @typedef {Object} FeedOptions                    -       The feed options and query methods.
	 * @property {string} [continuation]                -       Opaque token for continuing the enumeration.
	 * @property {boolean} [disableRUPerMinuteUsage]    -       DisableRUPerMinuteUsage is used to enable/disable Request Units(RUs)/minute capacity to serve the request if regular provisioned RUs/second is exhausted.
	 * @property {boolean} [enableCrossPartitionQuery]  -       A value indicating whether users are enabled to send more than one request to execute the query in the Azure Cosmos DB database service.
	                                                            <p>More than one request is necessary if the query is not scoped to single partition key value.</p>
	 * @property {boolean} [enableScanInQuery]          -       Allow scan on the queries which couldn't be served as indexing was opted out on the requested paths.
	 * @property {number} [maxDegreeOfParallelism]      -       The maximum number of concurrent operations that run client side during parallel query execution in the Azure Cosmos DB database service. Negative values make the system automatically decides the number of concurrent operations to run.
	 * @property {number} [maxItemCount]                -       Max number of items to be returned in the enumeration operation.
	 * @property {string} [partitionKey]                -       Specifies a partition key definition for a particular path in the Azure Cosmos DB database service.
	 * @property {string} [sessionToken]                -       Token for use with Session consistency.
	 */

	/**
	* The media options
	* @typedef {Object} MediaOptions                                          -         Options associated with upload media.
	* @property {string} [slug]                                               -         HTTP Slug header value.
	* @property {string} [contentType=application/octet-stream]               -         HTTP ContentType header value.
	*
	*/

	/**
	 * The Sql query parameter.
	 * @typedef {Object} SqlParameter
	 * @property {string} name         -       The name of the parameter.
	 * @property {string} value        -       The value of the parameter.
	 */

	/**
	* The Sql query specification.
	* @typedef {Object} SqlQuerySpec
	* @property {string} query                       -       The body of the query.
	* @property {Array<SqlParameter>} parameters     -       The array of {@link SqlParameter}.
	*/

	/**
	* The callback to execute after the request execution.
	* @callback RequestCallback
	* @param {object} error            -       Will contain error information if an error occurs, undefined otherwise.
	* @param {number} error.code       -       The response code corresponding to the error.
	* @param {string} error.body       -       A string represents the error information.
	* @param {Object} resource         -       An object that represents the requested resource (Db, collection, document ... etc) if no error happens.
	* @param {object} responseHeaders  -       An object that contain the response headers.
	*/

	/**
	* The Indexing Policy represents the indexing policy configuration for a collection.
	* @typedef {Object} IndexingPolicy
	* @property {boolean} automatic                                           -         Specifies whether automatic indexing is enabled for a collection.
	                                                                                   <p>In automatic indexing, documents can be explicitly excluded from indexing using {@link RequestOptions}.
	                                                                                   In manual indexing, documents can be explicitly included. </p>
	* @property {string} indexingMode                                         -         The indexing mode (consistent or lazy) {@link IndexingMode}.
	* @property {Array} IncludedPaths                                         -         An array of {@link IncludedPath} represents the paths to be included for indexing.
	* @property {Array} ExcludedPaths                                         -         An array of {@link ExcludedPath} represents the paths to be excluded from indexing.
	*
	*/

	/**
	* <p> Included path. <br>
	* </p>
	* @typedef {Object} IncludedPath
	* @property {Array} Indexes                                               -         An array of {@link Indexes}.
	* @property {string} Path                                                 -         Path to be indexed.
	*
	*/

	/**
	* <p> Index specification. <br>
	* </p>
	* @typedef {Object} Indexes
	* @property {string} Kind                                                  -         The index kind {@link IndexKind}.
	* @property {string} DataType                                              -         The data type {@link DataType}.
	* @property {number} Precision                                             -         The precision.
	*
	*/

	/**
	* <p> Excluded path. <br>
	* </p>
	* @typedef {Object} ExcludedPath
	* @property {string} Path                                                  -         Path to be indexed.
	*
	*/

	if (true) {
	    exports.DocumentClient = DocumentClient;
	    exports.DocumentBase = AzureDocuments;
	    exports.RetryOptions = RetryOptions;
	    exports.Base = Base;
	    exports.Constants = Constants;
	}


/***/ }),
/* 4 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var AuthHandler = __webpack_require__(9);
	var Constants = __webpack_require__(69);
	var Platform = __webpack_require__(70);

	//SCRIPT START
	function initializeProperties(target, members, prefix) {
	    var keys = Object.keys(members);
	    var properties;
	    var i, len;
	    for (i = 0, len = keys.length; i < len; i++) {
	        var key = keys[i];
	        var enumerable = key.charCodeAt(0) !== /*_*/ 95;
	        var member = members[key];
	        if (member && typeof member === "object") {
	            if (member.value !== undefined || typeof member.get === "function" || typeof member.set === "function") {
	                if (member.enumerable === undefined) {
	                    member.enumerable = enumerable;
	                }
	                if (prefix && member.setName && typeof member.setName === "function") {
	                    member.setName(prefix + "." + key);
	                }
	                properties = properties || {};
	                properties[key] = member;
	                continue;
	            }
	        }
	        if (!enumerable) {
	            properties = properties || {};
	            properties[key] = { value: member, enumerable: enumerable, configurable: true, writable: true };
	            continue;
	        }
	        target[key] = member;
	    }
	    if (properties) {
	        Object.defineProperties(target, properties);
	    }
	}

	/**
	*  Defines a new namespace with the specified name under the specified parent namespace.
	* @param {Object} parentNamespace - The parent namespace.
	* @param {String} name - The name of the new namespace.
	* @param {Object} members - The members of the new namespace.
	* @returns {Function} - The newly-defined namespace.
	*/
	function defineWithParent(parentNamespace, name, members) {
	    var currentNamespace = parentNamespace || {};
	    
	    if (name) {
	        var namespaceFragments = name.split(".");
	        for (var i = 0, len = namespaceFragments.length; i < len; i++) {
	            var namespaceName = namespaceFragments[i];
	            if (!currentNamespace[namespaceName]) {
	                Object.defineProperty(currentNamespace, namespaceName,
	                    { value: {}, writable: false, enumerable: true, configurable: true }
	                );
	            }
	            currentNamespace = currentNamespace[namespaceName];
	        }
	    }
	    
	    if (members) {
	        initializeProperties(currentNamespace, members, name || "<ANONYMOUS>");
	    }
	    
	    return currentNamespace;
	}

	/**
	*  Defines a new namespace with the specified name.
	* @param {String} name - The name of the namespace. This could be a dot-separated name for nested namespaces.
	* @param {Object} members - The members of the new namespace.
	* @returns {Function} - The newly-defined namespace.
	*/
	function define(name, members) {
	    return defineWithParent(undefined, name, members);
	}

	/**
	*  Defines a class using the given constructor and the specified instance members.
	* @param {Function} constructor - A constructor function that is used to instantiate this class.
	* @param {Object} instanceMembers - The set of instance fields, properties, and methods to be made available on the class.
	* @param {Object} staticMembers - The set of static fields, properties, and methods to be made available on the class.
	* @returns {Function} - The newly-defined class.
	*/
	function defineClass(constructor, instanceMembers, staticMembers) {
	    constructor = constructor || function () { };
	    if (instanceMembers) {
	        initializeProperties(constructor.prototype, instanceMembers);
	    }
	    if (staticMembers) {
	        initializeProperties(constructor, staticMembers);
	    }
	    return constructor;
	}

	/**
	*  Creates a sub-class based on the supplied baseClass parameter, using prototypal inheritance.
	* @param {Function} baseClass - The class to inherit from.
	* @param {Function} constructor - A constructor function that is used to instantiate this class.
	* @param {Object} instanceMembers - The set of instance fields, properties, and methods to be made available on the class.
	* @param {Object} staticMembers - The set of static fields, properties, and methods to be made available on the class.
	* @returns {Function} - The newly-defined class.
	*/
	function derive(baseClass, constructor, instanceMembers, staticMembers) {
	    if (baseClass) {
	        constructor = constructor || function () { };
	        var basePrototype = baseClass.prototype;
	        constructor.prototype = Object.create(basePrototype);
	        Object.defineProperty(constructor.prototype, "constructor", { value: constructor, writable: true, configurable: true, enumerable: true });
	        if (instanceMembers) {
	            initializeProperties(constructor.prototype, instanceMembers);
	        }
	        if (staticMembers) {
	            initializeProperties(constructor, staticMembers);
	        }
	        return constructor;
	    } else {
	        return defineClass(constructor, instanceMembers, staticMembers);
	    }
	}

	var Base = {
	    NotImplementedException: "NotImplementedException",
	    
	    defineWithParent: defineWithParent,
	    
	    define: define,
	    
	    defineClass: defineClass,
	    
	    derive: derive,
	    
	    extend: function (obj, extent) {
	        for (var property in extent) {
	            if (typeof extent[property] !== "function") {
	                obj[property] = extent[property];
	            }
	        }
	        return obj;
	    },
	    
	    map: function (list, fn) {
	        var result = [];
	        for (var i = 0, n = list.length; i < n; i++) {
	            result.push(fn(list[i]));
	        }
	        
	        return result;
	    },

	    /** @ignore */
	    jsonStringifyAndEscapeNonASCII: function (arg) {
	        // escapes non-ASCII characters as \uXXXX
	        return JSON.stringify(arg).replace(/[\u0080-\uFFFF]/g, function(m) {
	            return "\\u" + ("0000" + m.charCodeAt(0).toString(16)).slice(-4);
	        });
	    },

	    getHeaders: function (documentClient, defaultHeaders, verb, path, resourceId, resourceType, options, partitionKeyRangeId) {
	        
	        var headers = Base.extend({}, defaultHeaders);
	        options = options || {};
	        
	        if (options.continuation) {
	            headers[Constants.HttpHeaders.Continuation] = options.continuation;
	        }
	        
	        if (options.preTriggerInclude) {
	            headers[Constants.HttpHeaders.PreTriggerInclude] = options.preTriggerInclude.constructor === Array ? options.preTriggerInclude.join(",") : options.preTriggerInclude;
	        }
	        
	        if (options.postTriggerInclude) {
	            headers[Constants.HttpHeaders.PostTriggerInclude] = options.postTriggerInclude.constructor === Array ? options.postTriggerInclude.join(",") : options.postTriggerInclude;
	        }
	        
	        if (options.offerType) {
	            headers[Constants.HttpHeaders.OfferType] = options.offerType;
	        }
	        
	        if (options.offerThroughput) {
	            headers[Constants.HttpHeaders.OfferThroughput] = options.offerThroughput;
	        }
	        
	        if (options.maxItemCount) {
	            headers[Constants.HttpHeaders.PageSize] = options.maxItemCount;
	        }
	        
	        if (options.accessCondition) {
	            if (options.accessCondition.type === "IfMatch") {
	                headers[Constants.HttpHeaders.IfMatch] = options.accessCondition.condition;
	            } else {
	                headers[Constants.HttpHeaders.IfNoneMatch] = options.accessCondition.condition;
	            }
	        }
	        
	        if (options.indexingDirective) {
	            headers[Constants.HttpHeaders.IndexingDirective] = options.indexingDirective;
	        }
	        
	        // TODO: add consistency level validation.
	        if (options.consistencyLevel) {
	            headers[Constants.HttpHeaders.ConsistencyLevel] = options.consistencyLevel;
	        }
	        
	        if (options.resourceTokenExpirySeconds) {
	            headers[Constants.HttpHeaders.ResourceTokenExpiry] = options.resourceTokenExpirySeconds;
	        }
	        
	        // TODO: add session token automatic handling in case of session consistency.
	        if (options.sessionToken) {
	            headers[Constants.HttpHeaders.SessionToken] = options.sessionToken;
	        }
	        
	        if (options.enableScanInQuery) {
	            headers[Constants.HttpHeaders.EnableScanInQuery] = options.enableScanInQuery;
	        }
	        
	        if (options.enableCrossPartitionQuery) {
	            headers[Constants.HttpHeaders.EnableCrossPartitionQuery] = options.enableCrossPartitionQuery;
	        }

	        if (options.maxDegreeOfParallelism != undefined) {
	            headers[Constants.HttpHeaders.ParallelizeCrossPartitionQuery] = true;
	        }

	        if (options.populateQuotaInfo) {
	            headers[Constants.HttpHeaders.PopulateQuotaInfo] = true;
	        }
	        
	        // If the user is not using partition resolver, we add options.partitonKey to the header for elastic collections
	        if (documentClient.partitionResolver === undefined || documentClient.partitionResolver === null) {
	            if (options.partitionKey !== undefined) {
	                var partitionKey = options.partitionKey;
	                if (partitionKey === null || partitionKey.constructor !== Array) {
	                    partitionKey = [partitionKey];
	                }
	                headers[Constants.HttpHeaders.PartitionKey] = this.jsonStringifyAndEscapeNonASCII(partitionKey);
	            }
	        }
	        
	        if (documentClient.masterKey) {
	            headers[Constants.HttpHeaders.XDate] = new Date().toUTCString();
	        }
	        
	        if (documentClient.masterKey || documentClient.resourceTokens) {
	            headers[Constants.HttpHeaders.Authorization] = AuthHandler.getAuthorizationHeader(documentClient, verb, path, resourceId, resourceType, headers);
	        }
	        
	        if (verb === "post" || verb === "put") {
	            if (!headers[Constants.HttpHeaders.ContentType]) {
	                headers[Constants.HttpHeaders.ContentType] = Constants.MediaTypes.Json;
	            }
	        }
	        
	        if (!headers[Constants.HttpHeaders.Accept]) {
	            headers[Constants.HttpHeaders.Accept] = Constants.MediaTypes.Json;
	        }
	        
	        if (partitionKeyRangeId !== undefined) {
	            headers[Constants.HttpHeaders.PartitionKeyRangeID] = partitionKeyRangeId;
	        }

	        if (options.enableScriptLogging) {
	            headers[Constants.HttpHeaders.EnableScriptLogging] = options.enableScriptLogging;
	        }

	        if (options.offerEnableRUPerMinuteThroughput) {
	            headers[Constants.HttpHeaders.OfferIsRUPerMinuteThroughputEnabled] = true; 
	        }

	        if (options.disableRUPerMinuteUsage) {
	            headers[Constants.HttpHeaders.DisableRUPerMinuteUsage] = true; 
	        }

	        return headers;
	    },
	    
	    /** @ignore */
	    parseLink: function (resourcePath) {
	        if (resourcePath.length === 0) {
	            /* for DatabaseAccount case, both type and objectBody will be undefined. */
	            return {
	                type: undefined,
	                objectBody: undefined
	            };
	        }
	        
	        if (resourcePath[resourcePath.length - 1] !== "/") {
	            resourcePath = resourcePath + "/";
	        }
	        
	        if (resourcePath[0] !== "/") {
	            resourcePath = "/" + resourcePath;
	        }
	        
	        /*
	        / The path will be in the form of /[resourceType]/[resourceId]/ .... /[resourceType]//[resourceType]/[resourceId]/ .... /[resourceType]/[resourceId]/
	        / or /[resourceType]/[resourceId]/ .... /[resourceType]/[resourceId]/[resourceType]/[resourceId]/ .... /[resourceType]/[resourceId]/
	        / The result of split will be in the form of [[[resourceType], [resourceId] ... ,[resourceType], [resourceId], ""]
	        / In the first case, to extract the resourceId it will the element before last ( at length -2 ) and the the type will before it ( at length -3 )
	        / In the second case, to extract the resource type it will the element before last ( at length -2 )
	        */
	        var pathParts = resourcePath.split("/");
	        var id, type;
	        if (pathParts.length % 2 === 0) {
	            // request in form /[resourceType]/[resourceId]/ .... /[resourceType]/[resourceId].
	            id = pathParts[pathParts.length - 2];
	            type = pathParts[pathParts.length - 3];
	        } else {
	            // request in form /[resourceType]/[resourceId]/ .... /[resourceType]/.
	            id = pathParts[pathParts.length - 3];
	            type = pathParts[pathParts.length - 2];
	        }
	        
	        var result = {
	            type: type,
	            objectBody: {
	                id: id,
	                self: resourcePath
	            }
	        };
	        
	        return result;
	    },
	    
	    /** @ignore */
	    parsePath: function (path) {
	        var pathParts = [];
	        var currentIndex = 0;
	        
	        var throwError = function () {
	            throw new Error("Path " + path + " is invalid at index " + currentIndex);
	        };
	        
	        var getEscapedToken = function () {
	            var quote = path[currentIndex];
	            var newIndex = ++currentIndex;
	            
	            while (true) {
	                newIndex = path.indexOf(quote, newIndex);
	                if (newIndex == -1) {
	                    throwError();
	                }
	                
	                if (path[newIndex - 1] !== '\\') break;
	                
	                ++newIndex;
	            }
	            
	            var token = path.substr(currentIndex, newIndex - currentIndex);
	            currentIndex = newIndex + 1;
	            return token;
	        };
	        
	        var getToken = function () {
	            var newIndex = path.indexOf('/', currentIndex);
	            var token = null;
	            if (newIndex == -1) {
	                token = path.substr(currentIndex);
	                currentIndex = path.length;
	            }
	            else {
	                token = path.substr(currentIndex, newIndex - currentIndex);
	                currentIndex = newIndex;
	            }
	            
	            token = token.trim();
	            return token;
	        };
	        
	        while (currentIndex < path.length) {
	            if (path[currentIndex] !== '/') {
	                throwError();
	            }
	            
	            if (++currentIndex == path.length) break;
	            
	            if (path[currentIndex] === '\"' || path[currentIndex] === '\'') {
	                pathParts.push(getEscapedToken());
	            }
	            else {
	                pathParts.push(getToken());
	            }
	        }
	        
	        return pathParts;
	    },
	    
	    /** @ignore */
	    getDatabaseLink: function (link) {
	        return link.split('/').slice(0, 2).join('/');
	    },
	    
	    /** @ignore */
	    getCollectionLink: function (link) {
	        return link.split('/').slice(0, 4).join('/');
	    },
	    
	    /** @ignore */
	    getAttachmentIdFromMediaId: function (mediaId) {
	        // Replace - with / on the incoming mediaId.  This will preserve the / so that we can revert it later.
	        var buffer = new Buffer(mediaId.replace(/-/g, "/"), "base64");
	        var ResoureIdLength = 20;
	        var attachmentId = "";
	        if (buffer.length > ResoureIdLength) {
	            // After the base64 conversion, change the / back to a - to get the proper attachmentId
	            attachmentId = buffer.toString("base64", 0, ResoureIdLength).replace(/\//g, "-");
	        } else {
	            attachmentId = mediaId;
	        }
	        
	        return attachmentId;
	    },
	    
	    /** @ignore */
	    getHexaDigit: function () {
	        return Math.floor(Math.random() * 16).toString(16);
	    },
	    
	    /** @ignore */
	    generateGuidId: function () {
	        var id = "";
	        
	        for (var i = 0; i < 8; i++) {
	            id += Base.getHexaDigit();
	        }
	        
	        id += "-";
	        
	        for (var i = 0; i < 4; i++) {
	            id += Base.getHexaDigit();
	        }
	        
	        id += "-";
	        
	        for (var i = 0; i < 4; i++) {
	            id += Base.getHexaDigit();
	        }
	        
	        id += "-";
	        
	        for (var i = 0; i < 4; i++) {
	            id += Base.getHexaDigit();
	        }
	        
	        id += "-";
	        
	        for (var i = 0; i < 12; i++) {
	            id += Base.getHexaDigit();
	        }
	        
	        return id;
	    },
	    
	    isLinkNameBased: function (link) {
	        var parts = link.split("/");
	        var firstId = "";
	        var count = 0;
	        // Get the first id from path.
	        for (var i = 0; i < parts.length; ++i) {
	            if (!parts[i]) {
	                // Skip empty string.
	                continue;
	            }
	            ++count;
	            if (count === 1 && parts[i].toLowerCase() !== "dbs") {
	                return false;
	            }
	            if (count === 2) {
	                firstId = parts[i];
	                break;
	            }
	        }
	        if (!firstId) return false;
	        if (firstId.length !== 8) return true;
	        var decodedDataLength = Platform.getDecodedDataLength(firstId);
	        if (decodedDataLength !== 4) return true;
	        return false;
	    },
	    /** @ignore */
	    _trimSlashes: function (source) {
	        return source.replace(Constants.RegularExpressions.TrimLeftSlashes, "")
	                     .replace(Constants.RegularExpressions.TrimRightSlashes, "");
	    },
	    
	    /** @ignore */
	    _isValidCollectionLink: function (link) {
	        if (typeof link !== "string") {
	            return false;
	        }
	        
	        var parts = Base._trimSlashes(link).split("/");
	        
	        if (parts && parts.length !== 4) {
	            return false;
	        }
	        
	        if (parts[0] !== "dbs") {
	            return false;
	        }
	        
	        if (parts[2] !== "colls") {
	            return false;
	        }
	        
	        return true;
	    },
	};
	//SCRIPT END

	if (true) {
	    module.exports = Base;
	}

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 5 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(global) {/*!
	 * The buffer module from node.js, for the browser.
	 *
	 * @author   Feross Aboukhadijeh <feross@feross.org> <http://feross.org>
	 * @license  MIT
	 */
	/* eslint-disable no-proto */

	'use strict'

	var base64 = __webpack_require__(6)
	var ieee754 = __webpack_require__(7)
	var isArray = __webpack_require__(8)

	exports.Buffer = Buffer
	exports.SlowBuffer = SlowBuffer
	exports.INSPECT_MAX_BYTES = 50

	/**
	 * If `Buffer.TYPED_ARRAY_SUPPORT`:
	 *   === true    Use Uint8Array implementation (fastest)
	 *   === false   Use Object implementation (most compatible, even IE6)
	 *
	 * Browsers that support typed arrays are IE 10+, Firefox 4+, Chrome 7+, Safari 5.1+,
	 * Opera 11.6+, iOS 4.2+.
	 *
	 * Due to various browser bugs, sometimes the Object implementation will be used even
	 * when the browser supports typed arrays.
	 *
	 * Note:
	 *
	 *   - Firefox 4-29 lacks support for adding new properties to `Uint8Array` instances,
	 *     See: https://bugzilla.mozilla.org/show_bug.cgi?id=695438.
	 *
	 *   - Chrome 9-10 is missing the `TypedArray.prototype.subarray` function.
	 *
	 *   - IE10 has a broken `TypedArray.prototype.subarray` function which returns arrays of
	 *     incorrect length in some situations.

	 * We detect these buggy browsers and set `Buffer.TYPED_ARRAY_SUPPORT` to `false` so they
	 * get the Object implementation, which is slower but behaves correctly.
	 */
	Buffer.TYPED_ARRAY_SUPPORT = global.TYPED_ARRAY_SUPPORT !== undefined
	  ? global.TYPED_ARRAY_SUPPORT
	  : typedArraySupport()

	/*
	 * Export kMaxLength after typed array support is determined.
	 */
	exports.kMaxLength = kMaxLength()

	function typedArraySupport () {
	  try {
	    var arr = new Uint8Array(1)
	    arr.__proto__ = {__proto__: Uint8Array.prototype, foo: function () { return 42 }}
	    return arr.foo() === 42 && // typed array instances can be augmented
	        typeof arr.subarray === 'function' && // chrome 9-10 lack `subarray`
	        arr.subarray(1, 1).byteLength === 0 // ie10 has broken `subarray`
	  } catch (e) {
	    return false
	  }
	}

	function kMaxLength () {
	  return Buffer.TYPED_ARRAY_SUPPORT
	    ? 0x7fffffff
	    : 0x3fffffff
	}

	function createBuffer (that, length) {
	  if (kMaxLength() < length) {
	    throw new RangeError('Invalid typed array length')
	  }
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    // Return an augmented `Uint8Array` instance, for best performance
	    that = new Uint8Array(length)
	    that.__proto__ = Buffer.prototype
	  } else {
	    // Fallback: Return an object instance of the Buffer class
	    if (that === null) {
	      that = new Buffer(length)
	    }
	    that.length = length
	  }

	  return that
	}

	/**
	 * The Buffer constructor returns instances of `Uint8Array` that have their
	 * prototype changed to `Buffer.prototype`. Furthermore, `Buffer` is a subclass of
	 * `Uint8Array`, so the returned instances will have all the node `Buffer` methods
	 * and the `Uint8Array` methods. Square bracket notation works as expected -- it
	 * returns a single octet.
	 *
	 * The `Uint8Array` prototype remains unmodified.
	 */

	function Buffer (arg, encodingOrOffset, length) {
	  if (!Buffer.TYPED_ARRAY_SUPPORT && !(this instanceof Buffer)) {
	    return new Buffer(arg, encodingOrOffset, length)
	  }

	  // Common case.
	  if (typeof arg === 'number') {
	    if (typeof encodingOrOffset === 'string') {
	      throw new Error(
	        'If encoding is specified then the first argument must be a string'
	      )
	    }
	    return allocUnsafe(this, arg)
	  }
	  return from(this, arg, encodingOrOffset, length)
	}

	Buffer.poolSize = 8192 // not used by this implementation

	// TODO: Legacy, not needed anymore. Remove in next major version.
	Buffer._augment = function (arr) {
	  arr.__proto__ = Buffer.prototype
	  return arr
	}

	function from (that, value, encodingOrOffset, length) {
	  if (typeof value === 'number') {
	    throw new TypeError('"value" argument must not be a number')
	  }

	  if (typeof ArrayBuffer !== 'undefined' && value instanceof ArrayBuffer) {
	    return fromArrayBuffer(that, value, encodingOrOffset, length)
	  }

	  if (typeof value === 'string') {
	    return fromString(that, value, encodingOrOffset)
	  }

	  return fromObject(that, value)
	}

	/**
	 * Functionally equivalent to Buffer(arg, encoding) but throws a TypeError
	 * if value is a number.
	 * Buffer.from(str[, encoding])
	 * Buffer.from(array)
	 * Buffer.from(buffer)
	 * Buffer.from(arrayBuffer[, byteOffset[, length]])
	 **/
	Buffer.from = function (value, encodingOrOffset, length) {
	  return from(null, value, encodingOrOffset, length)
	}

	if (Buffer.TYPED_ARRAY_SUPPORT) {
	  Buffer.prototype.__proto__ = Uint8Array.prototype
	  Buffer.__proto__ = Uint8Array
	  if (typeof Symbol !== 'undefined' && Symbol.species &&
	      Buffer[Symbol.species] === Buffer) {
	    // Fix subarray() in ES2016. See: https://github.com/feross/buffer/pull/97
	    Object.defineProperty(Buffer, Symbol.species, {
	      value: null,
	      configurable: true
	    })
	  }
	}

	function assertSize (size) {
	  if (typeof size !== 'number') {
	    throw new TypeError('"size" argument must be a number')
	  } else if (size < 0) {
	    throw new RangeError('"size" argument must not be negative')
	  }
	}

	function alloc (that, size, fill, encoding) {
	  assertSize(size)
	  if (size <= 0) {
	    return createBuffer(that, size)
	  }
	  if (fill !== undefined) {
	    // Only pay attention to encoding if it's a string. This
	    // prevents accidentally sending in a number that would
	    // be interpretted as a start offset.
	    return typeof encoding === 'string'
	      ? createBuffer(that, size).fill(fill, encoding)
	      : createBuffer(that, size).fill(fill)
	  }
	  return createBuffer(that, size)
	}

	/**
	 * Creates a new filled Buffer instance.
	 * alloc(size[, fill[, encoding]])
	 **/
	Buffer.alloc = function (size, fill, encoding) {
	  return alloc(null, size, fill, encoding)
	}

	function allocUnsafe (that, size) {
	  assertSize(size)
	  that = createBuffer(that, size < 0 ? 0 : checked(size) | 0)
	  if (!Buffer.TYPED_ARRAY_SUPPORT) {
	    for (var i = 0; i < size; ++i) {
	      that[i] = 0
	    }
	  }
	  return that
	}

	/**
	 * Equivalent to Buffer(num), by default creates a non-zero-filled Buffer instance.
	 * */
	Buffer.allocUnsafe = function (size) {
	  return allocUnsafe(null, size)
	}
	/**
	 * Equivalent to SlowBuffer(num), by default creates a non-zero-filled Buffer instance.
	 */
	Buffer.allocUnsafeSlow = function (size) {
	  return allocUnsafe(null, size)
	}

	function fromString (that, string, encoding) {
	  if (typeof encoding !== 'string' || encoding === '') {
	    encoding = 'utf8'
	  }

	  if (!Buffer.isEncoding(encoding)) {
	    throw new TypeError('"encoding" must be a valid string encoding')
	  }

	  var length = byteLength(string, encoding) | 0
	  that = createBuffer(that, length)

	  var actual = that.write(string, encoding)

	  if (actual !== length) {
	    // Writing a hex string, for example, that contains invalid characters will
	    // cause everything after the first invalid character to be ignored. (e.g.
	    // 'abxxcd' will be treated as 'ab')
	    that = that.slice(0, actual)
	  }

	  return that
	}

	function fromArrayLike (that, array) {
	  var length = array.length < 0 ? 0 : checked(array.length) | 0
	  that = createBuffer(that, length)
	  for (var i = 0; i < length; i += 1) {
	    that[i] = array[i] & 255
	  }
	  return that
	}

	function fromArrayBuffer (that, array, byteOffset, length) {
	  array.byteLength // this throws if `array` is not a valid ArrayBuffer

	  if (byteOffset < 0 || array.byteLength < byteOffset) {
	    throw new RangeError('\'offset\' is out of bounds')
	  }

	  if (array.byteLength < byteOffset + (length || 0)) {
	    throw new RangeError('\'length\' is out of bounds')
	  }

	  if (byteOffset === undefined && length === undefined) {
	    array = new Uint8Array(array)
	  } else if (length === undefined) {
	    array = new Uint8Array(array, byteOffset)
	  } else {
	    array = new Uint8Array(array, byteOffset, length)
	  }

	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    // Return an augmented `Uint8Array` instance, for best performance
	    that = array
	    that.__proto__ = Buffer.prototype
	  } else {
	    // Fallback: Return an object instance of the Buffer class
	    that = fromArrayLike(that, array)
	  }
	  return that
	}

	function fromObject (that, obj) {
	  if (Buffer.isBuffer(obj)) {
	    var len = checked(obj.length) | 0
	    that = createBuffer(that, len)

	    if (that.length === 0) {
	      return that
	    }

	    obj.copy(that, 0, 0, len)
	    return that
	  }

	  if (obj) {
	    if ((typeof ArrayBuffer !== 'undefined' &&
	        obj.buffer instanceof ArrayBuffer) || 'length' in obj) {
	      if (typeof obj.length !== 'number' || isnan(obj.length)) {
	        return createBuffer(that, 0)
	      }
	      return fromArrayLike(that, obj)
	    }

	    if (obj.type === 'Buffer' && isArray(obj.data)) {
	      return fromArrayLike(that, obj.data)
	    }
	  }

	  throw new TypeError('First argument must be a string, Buffer, ArrayBuffer, Array, or array-like object.')
	}

	function checked (length) {
	  // Note: cannot use `length < kMaxLength()` here because that fails when
	  // length is NaN (which is otherwise coerced to zero.)
	  if (length >= kMaxLength()) {
	    throw new RangeError('Attempt to allocate Buffer larger than maximum ' +
	                         'size: 0x' + kMaxLength().toString(16) + ' bytes')
	  }
	  return length | 0
	}

	function SlowBuffer (length) {
	  if (+length != length) { // eslint-disable-line eqeqeq
	    length = 0
	  }
	  return Buffer.alloc(+length)
	}

	Buffer.isBuffer = function isBuffer (b) {
	  return !!(b != null && b._isBuffer)
	}

	Buffer.compare = function compare (a, b) {
	  if (!Buffer.isBuffer(a) || !Buffer.isBuffer(b)) {
	    throw new TypeError('Arguments must be Buffers')
	  }

	  if (a === b) return 0

	  var x = a.length
	  var y = b.length

	  for (var i = 0, len = Math.min(x, y); i < len; ++i) {
	    if (a[i] !== b[i]) {
	      x = a[i]
	      y = b[i]
	      break
	    }
	  }

	  if (x < y) return -1
	  if (y < x) return 1
	  return 0
	}

	Buffer.isEncoding = function isEncoding (encoding) {
	  switch (String(encoding).toLowerCase()) {
	    case 'hex':
	    case 'utf8':
	    case 'utf-8':
	    case 'ascii':
	    case 'latin1':
	    case 'binary':
	    case 'base64':
	    case 'ucs2':
	    case 'ucs-2':
	    case 'utf16le':
	    case 'utf-16le':
	      return true
	    default:
	      return false
	  }
	}

	Buffer.concat = function concat (list, length) {
	  if (!isArray(list)) {
	    throw new TypeError('"list" argument must be an Array of Buffers')
	  }

	  if (list.length === 0) {
	    return Buffer.alloc(0)
	  }

	  var i
	  if (length === undefined) {
	    length = 0
	    for (i = 0; i < list.length; ++i) {
	      length += list[i].length
	    }
	  }

	  var buffer = Buffer.allocUnsafe(length)
	  var pos = 0
	  for (i = 0; i < list.length; ++i) {
	    var buf = list[i]
	    if (!Buffer.isBuffer(buf)) {
	      throw new TypeError('"list" argument must be an Array of Buffers')
	    }
	    buf.copy(buffer, pos)
	    pos += buf.length
	  }
	  return buffer
	}

	function byteLength (string, encoding) {
	  if (Buffer.isBuffer(string)) {
	    return string.length
	  }
	  if (typeof ArrayBuffer !== 'undefined' && typeof ArrayBuffer.isView === 'function' &&
	      (ArrayBuffer.isView(string) || string instanceof ArrayBuffer)) {
	    return string.byteLength
	  }
	  if (typeof string !== 'string') {
	    string = '' + string
	  }

	  var len = string.length
	  if (len === 0) return 0

	  // Use a for loop to avoid recursion
	  var loweredCase = false
	  for (;;) {
	    switch (encoding) {
	      case 'ascii':
	      case 'latin1':
	      case 'binary':
	        return len
	      case 'utf8':
	      case 'utf-8':
	      case undefined:
	        return utf8ToBytes(string).length
	      case 'ucs2':
	      case 'ucs-2':
	      case 'utf16le':
	      case 'utf-16le':
	        return len * 2
	      case 'hex':
	        return len >>> 1
	      case 'base64':
	        return base64ToBytes(string).length
	      default:
	        if (loweredCase) return utf8ToBytes(string).length // assume utf8
	        encoding = ('' + encoding).toLowerCase()
	        loweredCase = true
	    }
	  }
	}
	Buffer.byteLength = byteLength

	function slowToString (encoding, start, end) {
	  var loweredCase = false

	  // No need to verify that "this.length <= MAX_UINT32" since it's a read-only
	  // property of a typed array.

	  // This behaves neither like String nor Uint8Array in that we set start/end
	  // to their upper/lower bounds if the value passed is out of range.
	  // undefined is handled specially as per ECMA-262 6th Edition,
	  // Section 13.3.3.7 Runtime Semantics: KeyedBindingInitialization.
	  if (start === undefined || start < 0) {
	    start = 0
	  }
	  // Return early if start > this.length. Done here to prevent potential uint32
	  // coercion fail below.
	  if (start > this.length) {
	    return ''
	  }

	  if (end === undefined || end > this.length) {
	    end = this.length
	  }

	  if (end <= 0) {
	    return ''
	  }

	  // Force coersion to uint32. This will also coerce falsey/NaN values to 0.
	  end >>>= 0
	  start >>>= 0

	  if (end <= start) {
	    return ''
	  }

	  if (!encoding) encoding = 'utf8'

	  while (true) {
	    switch (encoding) {
	      case 'hex':
	        return hexSlice(this, start, end)

	      case 'utf8':
	      case 'utf-8':
	        return utf8Slice(this, start, end)

	      case 'ascii':
	        return asciiSlice(this, start, end)

	      case 'latin1':
	      case 'binary':
	        return latin1Slice(this, start, end)

	      case 'base64':
	        return base64Slice(this, start, end)

	      case 'ucs2':
	      case 'ucs-2':
	      case 'utf16le':
	      case 'utf-16le':
	        return utf16leSlice(this, start, end)

	      default:
	        if (loweredCase) throw new TypeError('Unknown encoding: ' + encoding)
	        encoding = (encoding + '').toLowerCase()
	        loweredCase = true
	    }
	  }
	}

	// The property is used by `Buffer.isBuffer` and `is-buffer` (in Safari 5-7) to detect
	// Buffer instances.
	Buffer.prototype._isBuffer = true

	function swap (b, n, m) {
	  var i = b[n]
	  b[n] = b[m]
	  b[m] = i
	}

	Buffer.prototype.swap16 = function swap16 () {
	  var len = this.length
	  if (len % 2 !== 0) {
	    throw new RangeError('Buffer size must be a multiple of 16-bits')
	  }
	  for (var i = 0; i < len; i += 2) {
	    swap(this, i, i + 1)
	  }
	  return this
	}

	Buffer.prototype.swap32 = function swap32 () {
	  var len = this.length
	  if (len % 4 !== 0) {
	    throw new RangeError('Buffer size must be a multiple of 32-bits')
	  }
	  for (var i = 0; i < len; i += 4) {
	    swap(this, i, i + 3)
	    swap(this, i + 1, i + 2)
	  }
	  return this
	}

	Buffer.prototype.swap64 = function swap64 () {
	  var len = this.length
	  if (len % 8 !== 0) {
	    throw new RangeError('Buffer size must be a multiple of 64-bits')
	  }
	  for (var i = 0; i < len; i += 8) {
	    swap(this, i, i + 7)
	    swap(this, i + 1, i + 6)
	    swap(this, i + 2, i + 5)
	    swap(this, i + 3, i + 4)
	  }
	  return this
	}

	Buffer.prototype.toString = function toString () {
	  var length = this.length | 0
	  if (length === 0) return ''
	  if (arguments.length === 0) return utf8Slice(this, 0, length)
	  return slowToString.apply(this, arguments)
	}

	Buffer.prototype.equals = function equals (b) {
	  if (!Buffer.isBuffer(b)) throw new TypeError('Argument must be a Buffer')
	  if (this === b) return true
	  return Buffer.compare(this, b) === 0
	}

	Buffer.prototype.inspect = function inspect () {
	  var str = ''
	  var max = exports.INSPECT_MAX_BYTES
	  if (this.length > 0) {
	    str = this.toString('hex', 0, max).match(/.{2}/g).join(' ')
	    if (this.length > max) str += ' ... '
	  }
	  return '<Buffer ' + str + '>'
	}

	Buffer.prototype.compare = function compare (target, start, end, thisStart, thisEnd) {
	  if (!Buffer.isBuffer(target)) {
	    throw new TypeError('Argument must be a Buffer')
	  }

	  if (start === undefined) {
	    start = 0
	  }
	  if (end === undefined) {
	    end = target ? target.length : 0
	  }
	  if (thisStart === undefined) {
	    thisStart = 0
	  }
	  if (thisEnd === undefined) {
	    thisEnd = this.length
	  }

	  if (start < 0 || end > target.length || thisStart < 0 || thisEnd > this.length) {
	    throw new RangeError('out of range index')
	  }

	  if (thisStart >= thisEnd && start >= end) {
	    return 0
	  }
	  if (thisStart >= thisEnd) {
	    return -1
	  }
	  if (start >= end) {
	    return 1
	  }

	  start >>>= 0
	  end >>>= 0
	  thisStart >>>= 0
	  thisEnd >>>= 0

	  if (this === target) return 0

	  var x = thisEnd - thisStart
	  var y = end - start
	  var len = Math.min(x, y)

	  var thisCopy = this.slice(thisStart, thisEnd)
	  var targetCopy = target.slice(start, end)

	  for (var i = 0; i < len; ++i) {
	    if (thisCopy[i] !== targetCopy[i]) {
	      x = thisCopy[i]
	      y = targetCopy[i]
	      break
	    }
	  }

	  if (x < y) return -1
	  if (y < x) return 1
	  return 0
	}

	// Finds either the first index of `val` in `buffer` at offset >= `byteOffset`,
	// OR the last index of `val` in `buffer` at offset <= `byteOffset`.
	//
	// Arguments:
	// - buffer - a Buffer to search
	// - val - a string, Buffer, or number
	// - byteOffset - an index into `buffer`; will be clamped to an int32
	// - encoding - an optional encoding, relevant is val is a string
	// - dir - true for indexOf, false for lastIndexOf
	function bidirectionalIndexOf (buffer, val, byteOffset, encoding, dir) {
	  // Empty buffer means no match
	  if (buffer.length === 0) return -1

	  // Normalize byteOffset
	  if (typeof byteOffset === 'string') {
	    encoding = byteOffset
	    byteOffset = 0
	  } else if (byteOffset > 0x7fffffff) {
	    byteOffset = 0x7fffffff
	  } else if (byteOffset < -0x80000000) {
	    byteOffset = -0x80000000
	  }
	  byteOffset = +byteOffset  // Coerce to Number.
	  if (isNaN(byteOffset)) {
	    // byteOffset: it it's undefined, null, NaN, "foo", etc, search whole buffer
	    byteOffset = dir ? 0 : (buffer.length - 1)
	  }

	  // Normalize byteOffset: negative offsets start from the end of the buffer
	  if (byteOffset < 0) byteOffset = buffer.length + byteOffset
	  if (byteOffset >= buffer.length) {
	    if (dir) return -1
	    else byteOffset = buffer.length - 1
	  } else if (byteOffset < 0) {
	    if (dir) byteOffset = 0
	    else return -1
	  }

	  // Normalize val
	  if (typeof val === 'string') {
	    val = Buffer.from(val, encoding)
	  }

	  // Finally, search either indexOf (if dir is true) or lastIndexOf
	  if (Buffer.isBuffer(val)) {
	    // Special case: looking for empty string/buffer always fails
	    if (val.length === 0) {
	      return -1
	    }
	    return arrayIndexOf(buffer, val, byteOffset, encoding, dir)
	  } else if (typeof val === 'number') {
	    val = val & 0xFF // Search for a byte value [0-255]
	    if (Buffer.TYPED_ARRAY_SUPPORT &&
	        typeof Uint8Array.prototype.indexOf === 'function') {
	      if (dir) {
	        return Uint8Array.prototype.indexOf.call(buffer, val, byteOffset)
	      } else {
	        return Uint8Array.prototype.lastIndexOf.call(buffer, val, byteOffset)
	      }
	    }
	    return arrayIndexOf(buffer, [ val ], byteOffset, encoding, dir)
	  }

	  throw new TypeError('val must be string, number or Buffer')
	}

	function arrayIndexOf (arr, val, byteOffset, encoding, dir) {
	  var indexSize = 1
	  var arrLength = arr.length
	  var valLength = val.length

	  if (encoding !== undefined) {
	    encoding = String(encoding).toLowerCase()
	    if (encoding === 'ucs2' || encoding === 'ucs-2' ||
	        encoding === 'utf16le' || encoding === 'utf-16le') {
	      if (arr.length < 2 || val.length < 2) {
	        return -1
	      }
	      indexSize = 2
	      arrLength /= 2
	      valLength /= 2
	      byteOffset /= 2
	    }
	  }

	  function read (buf, i) {
	    if (indexSize === 1) {
	      return buf[i]
	    } else {
	      return buf.readUInt16BE(i * indexSize)
	    }
	  }

	  var i
	  if (dir) {
	    var foundIndex = -1
	    for (i = byteOffset; i < arrLength; i++) {
	      if (read(arr, i) === read(val, foundIndex === -1 ? 0 : i - foundIndex)) {
	        if (foundIndex === -1) foundIndex = i
	        if (i - foundIndex + 1 === valLength) return foundIndex * indexSize
	      } else {
	        if (foundIndex !== -1) i -= i - foundIndex
	        foundIndex = -1
	      }
	    }
	  } else {
	    if (byteOffset + valLength > arrLength) byteOffset = arrLength - valLength
	    for (i = byteOffset; i >= 0; i--) {
	      var found = true
	      for (var j = 0; j < valLength; j++) {
	        if (read(arr, i + j) !== read(val, j)) {
	          found = false
	          break
	        }
	      }
	      if (found) return i
	    }
	  }

	  return -1
	}

	Buffer.prototype.includes = function includes (val, byteOffset, encoding) {
	  return this.indexOf(val, byteOffset, encoding) !== -1
	}

	Buffer.prototype.indexOf = function indexOf (val, byteOffset, encoding) {
	  return bidirectionalIndexOf(this, val, byteOffset, encoding, true)
	}

	Buffer.prototype.lastIndexOf = function lastIndexOf (val, byteOffset, encoding) {
	  return bidirectionalIndexOf(this, val, byteOffset, encoding, false)
	}

	function hexWrite (buf, string, offset, length) {
	  offset = Number(offset) || 0
	  var remaining = buf.length - offset
	  if (!length) {
	    length = remaining
	  } else {
	    length = Number(length)
	    if (length > remaining) {
	      length = remaining
	    }
	  }

	  // must be an even number of digits
	  var strLen = string.length
	  if (strLen % 2 !== 0) throw new TypeError('Invalid hex string')

	  if (length > strLen / 2) {
	    length = strLen / 2
	  }
	  for (var i = 0; i < length; ++i) {
	    var parsed = parseInt(string.substr(i * 2, 2), 16)
	    if (isNaN(parsed)) return i
	    buf[offset + i] = parsed
	  }
	  return i
	}

	function utf8Write (buf, string, offset, length) {
	  return blitBuffer(utf8ToBytes(string, buf.length - offset), buf, offset, length)
	}

	function asciiWrite (buf, string, offset, length) {
	  return blitBuffer(asciiToBytes(string), buf, offset, length)
	}

	function latin1Write (buf, string, offset, length) {
	  return asciiWrite(buf, string, offset, length)
	}

	function base64Write (buf, string, offset, length) {
	  return blitBuffer(base64ToBytes(string), buf, offset, length)
	}

	function ucs2Write (buf, string, offset, length) {
	  return blitBuffer(utf16leToBytes(string, buf.length - offset), buf, offset, length)
	}

	Buffer.prototype.write = function write (string, offset, length, encoding) {
	  // Buffer#write(string)
	  if (offset === undefined) {
	    encoding = 'utf8'
	    length = this.length
	    offset = 0
	  // Buffer#write(string, encoding)
	  } else if (length === undefined && typeof offset === 'string') {
	    encoding = offset
	    length = this.length
	    offset = 0
	  // Buffer#write(string, offset[, length][, encoding])
	  } else if (isFinite(offset)) {
	    offset = offset | 0
	    if (isFinite(length)) {
	      length = length | 0
	      if (encoding === undefined) encoding = 'utf8'
	    } else {
	      encoding = length
	      length = undefined
	    }
	  // legacy write(string, encoding, offset, length) - remove in v0.13
	  } else {
	    throw new Error(
	      'Buffer.write(string, encoding, offset[, length]) is no longer supported'
	    )
	  }

	  var remaining = this.length - offset
	  if (length === undefined || length > remaining) length = remaining

	  if ((string.length > 0 && (length < 0 || offset < 0)) || offset > this.length) {
	    throw new RangeError('Attempt to write outside buffer bounds')
	  }

	  if (!encoding) encoding = 'utf8'

	  var loweredCase = false
	  for (;;) {
	    switch (encoding) {
	      case 'hex':
	        return hexWrite(this, string, offset, length)

	      case 'utf8':
	      case 'utf-8':
	        return utf8Write(this, string, offset, length)

	      case 'ascii':
	        return asciiWrite(this, string, offset, length)

	      case 'latin1':
	      case 'binary':
	        return latin1Write(this, string, offset, length)

	      case 'base64':
	        // Warning: maxLength not taken into account in base64Write
	        return base64Write(this, string, offset, length)

	      case 'ucs2':
	      case 'ucs-2':
	      case 'utf16le':
	      case 'utf-16le':
	        return ucs2Write(this, string, offset, length)

	      default:
	        if (loweredCase) throw new TypeError('Unknown encoding: ' + encoding)
	        encoding = ('' + encoding).toLowerCase()
	        loweredCase = true
	    }
	  }
	}

	Buffer.prototype.toJSON = function toJSON () {
	  return {
	    type: 'Buffer',
	    data: Array.prototype.slice.call(this._arr || this, 0)
	  }
	}

	function base64Slice (buf, start, end) {
	  if (start === 0 && end === buf.length) {
	    return base64.fromByteArray(buf)
	  } else {
	    return base64.fromByteArray(buf.slice(start, end))
	  }
	}

	function utf8Slice (buf, start, end) {
	  end = Math.min(buf.length, end)
	  var res = []

	  var i = start
	  while (i < end) {
	    var firstByte = buf[i]
	    var codePoint = null
	    var bytesPerSequence = (firstByte > 0xEF) ? 4
	      : (firstByte > 0xDF) ? 3
	      : (firstByte > 0xBF) ? 2
	      : 1

	    if (i + bytesPerSequence <= end) {
	      var secondByte, thirdByte, fourthByte, tempCodePoint

	      switch (bytesPerSequence) {
	        case 1:
	          if (firstByte < 0x80) {
	            codePoint = firstByte
	          }
	          break
	        case 2:
	          secondByte = buf[i + 1]
	          if ((secondByte & 0xC0) === 0x80) {
	            tempCodePoint = (firstByte & 0x1F) << 0x6 | (secondByte & 0x3F)
	            if (tempCodePoint > 0x7F) {
	              codePoint = tempCodePoint
	            }
	          }
	          break
	        case 3:
	          secondByte = buf[i + 1]
	          thirdByte = buf[i + 2]
	          if ((secondByte & 0xC0) === 0x80 && (thirdByte & 0xC0) === 0x80) {
	            tempCodePoint = (firstByte & 0xF) << 0xC | (secondByte & 0x3F) << 0x6 | (thirdByte & 0x3F)
	            if (tempCodePoint > 0x7FF && (tempCodePoint < 0xD800 || tempCodePoint > 0xDFFF)) {
	              codePoint = tempCodePoint
	            }
	          }
	          break
	        case 4:
	          secondByte = buf[i + 1]
	          thirdByte = buf[i + 2]
	          fourthByte = buf[i + 3]
	          if ((secondByte & 0xC0) === 0x80 && (thirdByte & 0xC0) === 0x80 && (fourthByte & 0xC0) === 0x80) {
	            tempCodePoint = (firstByte & 0xF) << 0x12 | (secondByte & 0x3F) << 0xC | (thirdByte & 0x3F) << 0x6 | (fourthByte & 0x3F)
	            if (tempCodePoint > 0xFFFF && tempCodePoint < 0x110000) {
	              codePoint = tempCodePoint
	            }
	          }
	      }
	    }

	    if (codePoint === null) {
	      // we did not generate a valid codePoint so insert a
	      // replacement char (U+FFFD) and advance only 1 byte
	      codePoint = 0xFFFD
	      bytesPerSequence = 1
	    } else if (codePoint > 0xFFFF) {
	      // encode to utf16 (surrogate pair dance)
	      codePoint -= 0x10000
	      res.push(codePoint >>> 10 & 0x3FF | 0xD800)
	      codePoint = 0xDC00 | codePoint & 0x3FF
	    }

	    res.push(codePoint)
	    i += bytesPerSequence
	  }

	  return decodeCodePointsArray(res)
	}

	// Based on http://stackoverflow.com/a/22747272/680742, the browser with
	// the lowest limit is Chrome, with 0x10000 args.
	// We go 1 magnitude less, for safety
	var MAX_ARGUMENTS_LENGTH = 0x1000

	function decodeCodePointsArray (codePoints) {
	  var len = codePoints.length
	  if (len <= MAX_ARGUMENTS_LENGTH) {
	    return String.fromCharCode.apply(String, codePoints) // avoid extra slice()
	  }

	  // Decode in chunks to avoid "call stack size exceeded".
	  var res = ''
	  var i = 0
	  while (i < len) {
	    res += String.fromCharCode.apply(
	      String,
	      codePoints.slice(i, i += MAX_ARGUMENTS_LENGTH)
	    )
	  }
	  return res
	}

	function asciiSlice (buf, start, end) {
	  var ret = ''
	  end = Math.min(buf.length, end)

	  for (var i = start; i < end; ++i) {
	    ret += String.fromCharCode(buf[i] & 0x7F)
	  }
	  return ret
	}

	function latin1Slice (buf, start, end) {
	  var ret = ''
	  end = Math.min(buf.length, end)

	  for (var i = start; i < end; ++i) {
	    ret += String.fromCharCode(buf[i])
	  }
	  return ret
	}

	function hexSlice (buf, start, end) {
	  var len = buf.length

	  if (!start || start < 0) start = 0
	  if (!end || end < 0 || end > len) end = len

	  var out = ''
	  for (var i = start; i < end; ++i) {
	    out += toHex(buf[i])
	  }
	  return out
	}

	function utf16leSlice (buf, start, end) {
	  var bytes = buf.slice(start, end)
	  var res = ''
	  for (var i = 0; i < bytes.length; i += 2) {
	    res += String.fromCharCode(bytes[i] + bytes[i + 1] * 256)
	  }
	  return res
	}

	Buffer.prototype.slice = function slice (start, end) {
	  var len = this.length
	  start = ~~start
	  end = end === undefined ? len : ~~end

	  if (start < 0) {
	    start += len
	    if (start < 0) start = 0
	  } else if (start > len) {
	    start = len
	  }

	  if (end < 0) {
	    end += len
	    if (end < 0) end = 0
	  } else if (end > len) {
	    end = len
	  }

	  if (end < start) end = start

	  var newBuf
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    newBuf = this.subarray(start, end)
	    newBuf.__proto__ = Buffer.prototype
	  } else {
	    var sliceLen = end - start
	    newBuf = new Buffer(sliceLen, undefined)
	    for (var i = 0; i < sliceLen; ++i) {
	      newBuf[i] = this[i + start]
	    }
	  }

	  return newBuf
	}

	/*
	 * Need to make sure that buffer isn't trying to write out of bounds.
	 */
	function checkOffset (offset, ext, length) {
	  if ((offset % 1) !== 0 || offset < 0) throw new RangeError('offset is not uint')
	  if (offset + ext > length) throw new RangeError('Trying to access beyond buffer length')
	}

	Buffer.prototype.readUIntLE = function readUIntLE (offset, byteLength, noAssert) {
	  offset = offset | 0
	  byteLength = byteLength | 0
	  if (!noAssert) checkOffset(offset, byteLength, this.length)

	  var val = this[offset]
	  var mul = 1
	  var i = 0
	  while (++i < byteLength && (mul *= 0x100)) {
	    val += this[offset + i] * mul
	  }

	  return val
	}

	Buffer.prototype.readUIntBE = function readUIntBE (offset, byteLength, noAssert) {
	  offset = offset | 0
	  byteLength = byteLength | 0
	  if (!noAssert) {
	    checkOffset(offset, byteLength, this.length)
	  }

	  var val = this[offset + --byteLength]
	  var mul = 1
	  while (byteLength > 0 && (mul *= 0x100)) {
	    val += this[offset + --byteLength] * mul
	  }

	  return val
	}

	Buffer.prototype.readUInt8 = function readUInt8 (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 1, this.length)
	  return this[offset]
	}

	Buffer.prototype.readUInt16LE = function readUInt16LE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 2, this.length)
	  return this[offset] | (this[offset + 1] << 8)
	}

	Buffer.prototype.readUInt16BE = function readUInt16BE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 2, this.length)
	  return (this[offset] << 8) | this[offset + 1]
	}

	Buffer.prototype.readUInt32LE = function readUInt32LE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 4, this.length)

	  return ((this[offset]) |
	      (this[offset + 1] << 8) |
	      (this[offset + 2] << 16)) +
	      (this[offset + 3] * 0x1000000)
	}

	Buffer.prototype.readUInt32BE = function readUInt32BE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 4, this.length)

	  return (this[offset] * 0x1000000) +
	    ((this[offset + 1] << 16) |
	    (this[offset + 2] << 8) |
	    this[offset + 3])
	}

	Buffer.prototype.readIntLE = function readIntLE (offset, byteLength, noAssert) {
	  offset = offset | 0
	  byteLength = byteLength | 0
	  if (!noAssert) checkOffset(offset, byteLength, this.length)

	  var val = this[offset]
	  var mul = 1
	  var i = 0
	  while (++i < byteLength && (mul *= 0x100)) {
	    val += this[offset + i] * mul
	  }
	  mul *= 0x80

	  if (val >= mul) val -= Math.pow(2, 8 * byteLength)

	  return val
	}

	Buffer.prototype.readIntBE = function readIntBE (offset, byteLength, noAssert) {
	  offset = offset | 0
	  byteLength = byteLength | 0
	  if (!noAssert) checkOffset(offset, byteLength, this.length)

	  var i = byteLength
	  var mul = 1
	  var val = this[offset + --i]
	  while (i > 0 && (mul *= 0x100)) {
	    val += this[offset + --i] * mul
	  }
	  mul *= 0x80

	  if (val >= mul) val -= Math.pow(2, 8 * byteLength)

	  return val
	}

	Buffer.prototype.readInt8 = function readInt8 (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 1, this.length)
	  if (!(this[offset] & 0x80)) return (this[offset])
	  return ((0xff - this[offset] + 1) * -1)
	}

	Buffer.prototype.readInt16LE = function readInt16LE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 2, this.length)
	  var val = this[offset] | (this[offset + 1] << 8)
	  return (val & 0x8000) ? val | 0xFFFF0000 : val
	}

	Buffer.prototype.readInt16BE = function readInt16BE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 2, this.length)
	  var val = this[offset + 1] | (this[offset] << 8)
	  return (val & 0x8000) ? val | 0xFFFF0000 : val
	}

	Buffer.prototype.readInt32LE = function readInt32LE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 4, this.length)

	  return (this[offset]) |
	    (this[offset + 1] << 8) |
	    (this[offset + 2] << 16) |
	    (this[offset + 3] << 24)
	}

	Buffer.prototype.readInt32BE = function readInt32BE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 4, this.length)

	  return (this[offset] << 24) |
	    (this[offset + 1] << 16) |
	    (this[offset + 2] << 8) |
	    (this[offset + 3])
	}

	Buffer.prototype.readFloatLE = function readFloatLE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 4, this.length)
	  return ieee754.read(this, offset, true, 23, 4)
	}

	Buffer.prototype.readFloatBE = function readFloatBE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 4, this.length)
	  return ieee754.read(this, offset, false, 23, 4)
	}

	Buffer.prototype.readDoubleLE = function readDoubleLE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 8, this.length)
	  return ieee754.read(this, offset, true, 52, 8)
	}

	Buffer.prototype.readDoubleBE = function readDoubleBE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 8, this.length)
	  return ieee754.read(this, offset, false, 52, 8)
	}

	function checkInt (buf, value, offset, ext, max, min) {
	  if (!Buffer.isBuffer(buf)) throw new TypeError('"buffer" argument must be a Buffer instance')
	  if (value > max || value < min) throw new RangeError('"value" argument is out of bounds')
	  if (offset + ext > buf.length) throw new RangeError('Index out of range')
	}

	Buffer.prototype.writeUIntLE = function writeUIntLE (value, offset, byteLength, noAssert) {
	  value = +value
	  offset = offset | 0
	  byteLength = byteLength | 0
	  if (!noAssert) {
	    var maxBytes = Math.pow(2, 8 * byteLength) - 1
	    checkInt(this, value, offset, byteLength, maxBytes, 0)
	  }

	  var mul = 1
	  var i = 0
	  this[offset] = value & 0xFF
	  while (++i < byteLength && (mul *= 0x100)) {
	    this[offset + i] = (value / mul) & 0xFF
	  }

	  return offset + byteLength
	}

	Buffer.prototype.writeUIntBE = function writeUIntBE (value, offset, byteLength, noAssert) {
	  value = +value
	  offset = offset | 0
	  byteLength = byteLength | 0
	  if (!noAssert) {
	    var maxBytes = Math.pow(2, 8 * byteLength) - 1
	    checkInt(this, value, offset, byteLength, maxBytes, 0)
	  }

	  var i = byteLength - 1
	  var mul = 1
	  this[offset + i] = value & 0xFF
	  while (--i >= 0 && (mul *= 0x100)) {
	    this[offset + i] = (value / mul) & 0xFF
	  }

	  return offset + byteLength
	}

	Buffer.prototype.writeUInt8 = function writeUInt8 (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 1, 0xff, 0)
	  if (!Buffer.TYPED_ARRAY_SUPPORT) value = Math.floor(value)
	  this[offset] = (value & 0xff)
	  return offset + 1
	}

	function objectWriteUInt16 (buf, value, offset, littleEndian) {
	  if (value < 0) value = 0xffff + value + 1
	  for (var i = 0, j = Math.min(buf.length - offset, 2); i < j; ++i) {
	    buf[offset + i] = (value & (0xff << (8 * (littleEndian ? i : 1 - i)))) >>>
	      (littleEndian ? i : 1 - i) * 8
	  }
	}

	Buffer.prototype.writeUInt16LE = function writeUInt16LE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 2, 0xffff, 0)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value & 0xff)
	    this[offset + 1] = (value >>> 8)
	  } else {
	    objectWriteUInt16(this, value, offset, true)
	  }
	  return offset + 2
	}

	Buffer.prototype.writeUInt16BE = function writeUInt16BE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 2, 0xffff, 0)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value >>> 8)
	    this[offset + 1] = (value & 0xff)
	  } else {
	    objectWriteUInt16(this, value, offset, false)
	  }
	  return offset + 2
	}

	function objectWriteUInt32 (buf, value, offset, littleEndian) {
	  if (value < 0) value = 0xffffffff + value + 1
	  for (var i = 0, j = Math.min(buf.length - offset, 4); i < j; ++i) {
	    buf[offset + i] = (value >>> (littleEndian ? i : 3 - i) * 8) & 0xff
	  }
	}

	Buffer.prototype.writeUInt32LE = function writeUInt32LE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 4, 0xffffffff, 0)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset + 3] = (value >>> 24)
	    this[offset + 2] = (value >>> 16)
	    this[offset + 1] = (value >>> 8)
	    this[offset] = (value & 0xff)
	  } else {
	    objectWriteUInt32(this, value, offset, true)
	  }
	  return offset + 4
	}

	Buffer.prototype.writeUInt32BE = function writeUInt32BE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 4, 0xffffffff, 0)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value >>> 24)
	    this[offset + 1] = (value >>> 16)
	    this[offset + 2] = (value >>> 8)
	    this[offset + 3] = (value & 0xff)
	  } else {
	    objectWriteUInt32(this, value, offset, false)
	  }
	  return offset + 4
	}

	Buffer.prototype.writeIntLE = function writeIntLE (value, offset, byteLength, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) {
	    var limit = Math.pow(2, 8 * byteLength - 1)

	    checkInt(this, value, offset, byteLength, limit - 1, -limit)
	  }

	  var i = 0
	  var mul = 1
	  var sub = 0
	  this[offset] = value & 0xFF
	  while (++i < byteLength && (mul *= 0x100)) {
	    if (value < 0 && sub === 0 && this[offset + i - 1] !== 0) {
	      sub = 1
	    }
	    this[offset + i] = ((value / mul) >> 0) - sub & 0xFF
	  }

	  return offset + byteLength
	}

	Buffer.prototype.writeIntBE = function writeIntBE (value, offset, byteLength, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) {
	    var limit = Math.pow(2, 8 * byteLength - 1)

	    checkInt(this, value, offset, byteLength, limit - 1, -limit)
	  }

	  var i = byteLength - 1
	  var mul = 1
	  var sub = 0
	  this[offset + i] = value & 0xFF
	  while (--i >= 0 && (mul *= 0x100)) {
	    if (value < 0 && sub === 0 && this[offset + i + 1] !== 0) {
	      sub = 1
	    }
	    this[offset + i] = ((value / mul) >> 0) - sub & 0xFF
	  }

	  return offset + byteLength
	}

	Buffer.prototype.writeInt8 = function writeInt8 (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 1, 0x7f, -0x80)
	  if (!Buffer.TYPED_ARRAY_SUPPORT) value = Math.floor(value)
	  if (value < 0) value = 0xff + value + 1
	  this[offset] = (value & 0xff)
	  return offset + 1
	}

	Buffer.prototype.writeInt16LE = function writeInt16LE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 2, 0x7fff, -0x8000)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value & 0xff)
	    this[offset + 1] = (value >>> 8)
	  } else {
	    objectWriteUInt16(this, value, offset, true)
	  }
	  return offset + 2
	}

	Buffer.prototype.writeInt16BE = function writeInt16BE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 2, 0x7fff, -0x8000)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value >>> 8)
	    this[offset + 1] = (value & 0xff)
	  } else {
	    objectWriteUInt16(this, value, offset, false)
	  }
	  return offset + 2
	}

	Buffer.prototype.writeInt32LE = function writeInt32LE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 4, 0x7fffffff, -0x80000000)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value & 0xff)
	    this[offset + 1] = (value >>> 8)
	    this[offset + 2] = (value >>> 16)
	    this[offset + 3] = (value >>> 24)
	  } else {
	    objectWriteUInt32(this, value, offset, true)
	  }
	  return offset + 4
	}

	Buffer.prototype.writeInt32BE = function writeInt32BE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 4, 0x7fffffff, -0x80000000)
	  if (value < 0) value = 0xffffffff + value + 1
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value >>> 24)
	    this[offset + 1] = (value >>> 16)
	    this[offset + 2] = (value >>> 8)
	    this[offset + 3] = (value & 0xff)
	  } else {
	    objectWriteUInt32(this, value, offset, false)
	  }
	  return offset + 4
	}

	function checkIEEE754 (buf, value, offset, ext, max, min) {
	  if (offset + ext > buf.length) throw new RangeError('Index out of range')
	  if (offset < 0) throw new RangeError('Index out of range')
	}

	function writeFloat (buf, value, offset, littleEndian, noAssert) {
	  if (!noAssert) {
	    checkIEEE754(buf, value, offset, 4, 3.4028234663852886e+38, -3.4028234663852886e+38)
	  }
	  ieee754.write(buf, value, offset, littleEndian, 23, 4)
	  return offset + 4
	}

	Buffer.prototype.writeFloatLE = function writeFloatLE (value, offset, noAssert) {
	  return writeFloat(this, value, offset, true, noAssert)
	}

	Buffer.prototype.writeFloatBE = function writeFloatBE (value, offset, noAssert) {
	  return writeFloat(this, value, offset, false, noAssert)
	}

	function writeDouble (buf, value, offset, littleEndian, noAssert) {
	  if (!noAssert) {
	    checkIEEE754(buf, value, offset, 8, 1.7976931348623157E+308, -1.7976931348623157E+308)
	  }
	  ieee754.write(buf, value, offset, littleEndian, 52, 8)
	  return offset + 8
	}

	Buffer.prototype.writeDoubleLE = function writeDoubleLE (value, offset, noAssert) {
	  return writeDouble(this, value, offset, true, noAssert)
	}

	Buffer.prototype.writeDoubleBE = function writeDoubleBE (value, offset, noAssert) {
	  return writeDouble(this, value, offset, false, noAssert)
	}

	// copy(targetBuffer, targetStart=0, sourceStart=0, sourceEnd=buffer.length)
	Buffer.prototype.copy = function copy (target, targetStart, start, end) {
	  if (!start) start = 0
	  if (!end && end !== 0) end = this.length
	  if (targetStart >= target.length) targetStart = target.length
	  if (!targetStart) targetStart = 0
	  if (end > 0 && end < start) end = start

	  // Copy 0 bytes; we're done
	  if (end === start) return 0
	  if (target.length === 0 || this.length === 0) return 0

	  // Fatal error conditions
	  if (targetStart < 0) {
	    throw new RangeError('targetStart out of bounds')
	  }
	  if (start < 0 || start >= this.length) throw new RangeError('sourceStart out of bounds')
	  if (end < 0) throw new RangeError('sourceEnd out of bounds')

	  // Are we oob?
	  if (end > this.length) end = this.length
	  if (target.length - targetStart < end - start) {
	    end = target.length - targetStart + start
	  }

	  var len = end - start
	  var i

	  if (this === target && start < targetStart && targetStart < end) {
	    // descending copy from end
	    for (i = len - 1; i >= 0; --i) {
	      target[i + targetStart] = this[i + start]
	    }
	  } else if (len < 1000 || !Buffer.TYPED_ARRAY_SUPPORT) {
	    // ascending copy from start
	    for (i = 0; i < len; ++i) {
	      target[i + targetStart] = this[i + start]
	    }
	  } else {
	    Uint8Array.prototype.set.call(
	      target,
	      this.subarray(start, start + len),
	      targetStart
	    )
	  }

	  return len
	}

	// Usage:
	//    buffer.fill(number[, offset[, end]])
	//    buffer.fill(buffer[, offset[, end]])
	//    buffer.fill(string[, offset[, end]][, encoding])
	Buffer.prototype.fill = function fill (val, start, end, encoding) {
	  // Handle string cases:
	  if (typeof val === 'string') {
	    if (typeof start === 'string') {
	      encoding = start
	      start = 0
	      end = this.length
	    } else if (typeof end === 'string') {
	      encoding = end
	      end = this.length
	    }
	    if (val.length === 1) {
	      var code = val.charCodeAt(0)
	      if (code < 256) {
	        val = code
	      }
	    }
	    if (encoding !== undefined && typeof encoding !== 'string') {
	      throw new TypeError('encoding must be a string')
	    }
	    if (typeof encoding === 'string' && !Buffer.isEncoding(encoding)) {
	      throw new TypeError('Unknown encoding: ' + encoding)
	    }
	  } else if (typeof val === 'number') {
	    val = val & 255
	  }

	  // Invalid ranges are not set to a default, so can range check early.
	  if (start < 0 || this.length < start || this.length < end) {
	    throw new RangeError('Out of range index')
	  }

	  if (end <= start) {
	    return this
	  }

	  start = start >>> 0
	  end = end === undefined ? this.length : end >>> 0

	  if (!val) val = 0

	  var i
	  if (typeof val === 'number') {
	    for (i = start; i < end; ++i) {
	      this[i] = val
	    }
	  } else {
	    var bytes = Buffer.isBuffer(val)
	      ? val
	      : utf8ToBytes(new Buffer(val, encoding).toString())
	    var len = bytes.length
	    for (i = 0; i < end - start; ++i) {
	      this[i + start] = bytes[i % len]
	    }
	  }

	  return this
	}

	// HELPER FUNCTIONS
	// ================

	var INVALID_BASE64_RE = /[^+\/0-9A-Za-z-_]/g

	function base64clean (str) {
	  // Node strips out invalid characters like \n and \t from the string, base64-js does not
	  str = stringtrim(str).replace(INVALID_BASE64_RE, '')
	  // Node converts strings with length < 2 to ''
	  if (str.length < 2) return ''
	  // Node allows for non-padded base64 strings (missing trailing ===), base64-js does not
	  while (str.length % 4 !== 0) {
	    str = str + '='
	  }
	  return str
	}

	function stringtrim (str) {
	  if (str.trim) return str.trim()
	  return str.replace(/^\s+|\s+$/g, '')
	}

	function toHex (n) {
	  if (n < 16) return '0' + n.toString(16)
	  return n.toString(16)
	}

	function utf8ToBytes (string, units) {
	  units = units || Infinity
	  var codePoint
	  var length = string.length
	  var leadSurrogate = null
	  var bytes = []

	  for (var i = 0; i < length; ++i) {
	    codePoint = string.charCodeAt(i)

	    // is surrogate component
	    if (codePoint > 0xD7FF && codePoint < 0xE000) {
	      // last char was a lead
	      if (!leadSurrogate) {
	        // no lead yet
	        if (codePoint > 0xDBFF) {
	          // unexpected trail
	          if ((units -= 3) > -1) bytes.push(0xEF, 0xBF, 0xBD)
	          continue
	        } else if (i + 1 === length) {
	          // unpaired lead
	          if ((units -= 3) > -1) bytes.push(0xEF, 0xBF, 0xBD)
	          continue
	        }

	        // valid lead
	        leadSurrogate = codePoint

	        continue
	      }

	      // 2 leads in a row
	      if (codePoint < 0xDC00) {
	        if ((units -= 3) > -1) bytes.push(0xEF, 0xBF, 0xBD)
	        leadSurrogate = codePoint
	        continue
	      }

	      // valid surrogate pair
	      codePoint = (leadSurrogate - 0xD800 << 10 | codePoint - 0xDC00) + 0x10000
	    } else if (leadSurrogate) {
	      // valid bmp char, but last char was a lead
	      if ((units -= 3) > -1) bytes.push(0xEF, 0xBF, 0xBD)
	    }

	    leadSurrogate = null

	    // encode utf8
	    if (codePoint < 0x80) {
	      if ((units -= 1) < 0) break
	      bytes.push(codePoint)
	    } else if (codePoint < 0x800) {
	      if ((units -= 2) < 0) break
	      bytes.push(
	        codePoint >> 0x6 | 0xC0,
	        codePoint & 0x3F | 0x80
	      )
	    } else if (codePoint < 0x10000) {
	      if ((units -= 3) < 0) break
	      bytes.push(
	        codePoint >> 0xC | 0xE0,
	        codePoint >> 0x6 & 0x3F | 0x80,
	        codePoint & 0x3F | 0x80
	      )
	    } else if (codePoint < 0x110000) {
	      if ((units -= 4) < 0) break
	      bytes.push(
	        codePoint >> 0x12 | 0xF0,
	        codePoint >> 0xC & 0x3F | 0x80,
	        codePoint >> 0x6 & 0x3F | 0x80,
	        codePoint & 0x3F | 0x80
	      )
	    } else {
	      throw new Error('Invalid code point')
	    }
	  }

	  return bytes
	}

	function asciiToBytes (str) {
	  var byteArray = []
	  for (var i = 0; i < str.length; ++i) {
	    // Node's code seems to be doing this and not & 0x7F..
	    byteArray.push(str.charCodeAt(i) & 0xFF)
	  }
	  return byteArray
	}

	function utf16leToBytes (str, units) {
	  var c, hi, lo
	  var byteArray = []
	  for (var i = 0; i < str.length; ++i) {
	    if ((units -= 2) < 0) break

	    c = str.charCodeAt(i)
	    hi = c >> 8
	    lo = c % 256
	    byteArray.push(lo)
	    byteArray.push(hi)
	  }

	  return byteArray
	}

	function base64ToBytes (str) {
	  return base64.toByteArray(base64clean(str))
	}

	function blitBuffer (src, dst, offset, length) {
	  for (var i = 0; i < length; ++i) {
	    if ((i + offset >= dst.length) || (i >= src.length)) break
	    dst[i + offset] = src[i]
	  }
	  return i
	}

	function isnan (val) {
	  return val !== val // eslint-disable-line no-self-compare
	}

	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }())))

/***/ }),
/* 6 */
/***/ (function(module, exports) {

	'use strict'

	exports.byteLength = byteLength
	exports.toByteArray = toByteArray
	exports.fromByteArray = fromByteArray

	var lookup = []
	var revLookup = []
	var Arr = typeof Uint8Array !== 'undefined' ? Uint8Array : Array

	var code = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
	for (var i = 0, len = code.length; i < len; ++i) {
	  lookup[i] = code[i]
	  revLookup[code.charCodeAt(i)] = i
	}

	// Support decoding URL-safe base64 strings, as Node.js does.
	// See: https://en.wikipedia.org/wiki/Base64#URL_applications
	revLookup['-'.charCodeAt(0)] = 62
	revLookup['_'.charCodeAt(0)] = 63

	function placeHoldersCount (b64) {
	  var len = b64.length
	  if (len % 4 > 0) {
	    throw new Error('Invalid string. Length must be a multiple of 4')
	  }

	  // the number of equal signs (place holders)
	  // if there are two placeholders, than the two characters before it
	  // represent one byte
	  // if there is only one, then the three characters before it represent 2 bytes
	  // this is just a cheap hack to not do indexOf twice
	  return b64[len - 2] === '=' ? 2 : b64[len - 1] === '=' ? 1 : 0
	}

	function byteLength (b64) {
	  // base64 is 4/3 + up to two characters of the original data
	  return (b64.length * 3 / 4) - placeHoldersCount(b64)
	}

	function toByteArray (b64) {
	  var i, l, tmp, placeHolders, arr
	  var len = b64.length
	  placeHolders = placeHoldersCount(b64)

	  arr = new Arr((len * 3 / 4) - placeHolders)

	  // if there are placeholders, only get up to the last complete 4 chars
	  l = placeHolders > 0 ? len - 4 : len

	  var L = 0

	  for (i = 0; i < l; i += 4) {
	    tmp = (revLookup[b64.charCodeAt(i)] << 18) | (revLookup[b64.charCodeAt(i + 1)] << 12) | (revLookup[b64.charCodeAt(i + 2)] << 6) | revLookup[b64.charCodeAt(i + 3)]
	    arr[L++] = (tmp >> 16) & 0xFF
	    arr[L++] = (tmp >> 8) & 0xFF
	    arr[L++] = tmp & 0xFF
	  }

	  if (placeHolders === 2) {
	    tmp = (revLookup[b64.charCodeAt(i)] << 2) | (revLookup[b64.charCodeAt(i + 1)] >> 4)
	    arr[L++] = tmp & 0xFF
	  } else if (placeHolders === 1) {
	    tmp = (revLookup[b64.charCodeAt(i)] << 10) | (revLookup[b64.charCodeAt(i + 1)] << 4) | (revLookup[b64.charCodeAt(i + 2)] >> 2)
	    arr[L++] = (tmp >> 8) & 0xFF
	    arr[L++] = tmp & 0xFF
	  }

	  return arr
	}

	function tripletToBase64 (num) {
	  return lookup[num >> 18 & 0x3F] + lookup[num >> 12 & 0x3F] + lookup[num >> 6 & 0x3F] + lookup[num & 0x3F]
	}

	function encodeChunk (uint8, start, end) {
	  var tmp
	  var output = []
	  for (var i = start; i < end; i += 3) {
	    tmp = ((uint8[i] << 16) & 0xFF0000) + ((uint8[i + 1] << 8) & 0xFF00) + (uint8[i + 2] & 0xFF)
	    output.push(tripletToBase64(tmp))
	  }
	  return output.join('')
	}

	function fromByteArray (uint8) {
	  var tmp
	  var len = uint8.length
	  var extraBytes = len % 3 // if we have 1 byte left, pad 2 bytes
	  var output = ''
	  var parts = []
	  var maxChunkLength = 16383 // must be multiple of 3

	  // go through the array every three bytes, we'll deal with trailing stuff later
	  for (var i = 0, len2 = len - extraBytes; i < len2; i += maxChunkLength) {
	    parts.push(encodeChunk(uint8, i, (i + maxChunkLength) > len2 ? len2 : (i + maxChunkLength)))
	  }

	  // pad the end with zeros, but make sure to not forget the extra bytes
	  if (extraBytes === 1) {
	    tmp = uint8[len - 1]
	    output += lookup[tmp >> 2]
	    output += lookup[(tmp << 4) & 0x3F]
	    output += '=='
	  } else if (extraBytes === 2) {
	    tmp = (uint8[len - 2] << 8) + (uint8[len - 1])
	    output += lookup[tmp >> 10]
	    output += lookup[(tmp >> 4) & 0x3F]
	    output += lookup[(tmp << 2) & 0x3F]
	    output += '='
	  }

	  parts.push(output)

	  return parts.join('')
	}


/***/ }),
/* 7 */
/***/ (function(module, exports) {

	exports.read = function (buffer, offset, isLE, mLen, nBytes) {
	  var e, m
	  var eLen = nBytes * 8 - mLen - 1
	  var eMax = (1 << eLen) - 1
	  var eBias = eMax >> 1
	  var nBits = -7
	  var i = isLE ? (nBytes - 1) : 0
	  var d = isLE ? -1 : 1
	  var s = buffer[offset + i]

	  i += d

	  e = s & ((1 << (-nBits)) - 1)
	  s >>= (-nBits)
	  nBits += eLen
	  for (; nBits > 0; e = e * 256 + buffer[offset + i], i += d, nBits -= 8) {}

	  m = e & ((1 << (-nBits)) - 1)
	  e >>= (-nBits)
	  nBits += mLen
	  for (; nBits > 0; m = m * 256 + buffer[offset + i], i += d, nBits -= 8) {}

	  if (e === 0) {
	    e = 1 - eBias
	  } else if (e === eMax) {
	    return m ? NaN : ((s ? -1 : 1) * Infinity)
	  } else {
	    m = m + Math.pow(2, mLen)
	    e = e - eBias
	  }
	  return (s ? -1 : 1) * m * Math.pow(2, e - mLen)
	}

	exports.write = function (buffer, value, offset, isLE, mLen, nBytes) {
	  var e, m, c
	  var eLen = nBytes * 8 - mLen - 1
	  var eMax = (1 << eLen) - 1
	  var eBias = eMax >> 1
	  var rt = (mLen === 23 ? Math.pow(2, -24) - Math.pow(2, -77) : 0)
	  var i = isLE ? 0 : (nBytes - 1)
	  var d = isLE ? 1 : -1
	  var s = value < 0 || (value === 0 && 1 / value < 0) ? 1 : 0

	  value = Math.abs(value)

	  if (isNaN(value) || value === Infinity) {
	    m = isNaN(value) ? 1 : 0
	    e = eMax
	  } else {
	    e = Math.floor(Math.log(value) / Math.LN2)
	    if (value * (c = Math.pow(2, -e)) < 1) {
	      e--
	      c *= 2
	    }
	    if (e + eBias >= 1) {
	      value += rt / c
	    } else {
	      value += rt * Math.pow(2, 1 - eBias)
	    }
	    if (value * c >= 2) {
	      e++
	      c /= 2
	    }

	    if (e + eBias >= eMax) {
	      m = 0
	      e = eMax
	    } else if (e + eBias >= 1) {
	      m = (value * c - 1) * Math.pow(2, mLen)
	      e = e + eBias
	    } else {
	      m = value * Math.pow(2, eBias - 1) * Math.pow(2, mLen)
	      e = 0
	    }
	  }

	  for (; mLen >= 8; buffer[offset + i] = m & 0xff, i += d, m /= 256, mLen -= 8) {}

	  e = (e << mLen) | m
	  eLen += mLen
	  for (; eLen > 0; buffer[offset + i] = e & 0xff, i += d, e /= 256, eLen -= 8) {}

	  buffer[offset + i - d] |= s * 128
	}


/***/ }),
/* 8 */
/***/ (function(module, exports) {

	var toString = {}.toString;

	module.exports = Array.isArray || function (arr) {
	  return toString.call(arr) == '[object Array]';
	};


/***/ }),
/* 9 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var crypto = __webpack_require__(10);

	var AuthHandler = {
	    getAuthorizationHeader: function (documentClient, verb, path, resourceId, resourceType, headers) {
	        if (documentClient.masterKey) {
	            return encodeURIComponent(this.getAuthorizationTokenUsingMasterKey(verb, resourceId, resourceType, headers, documentClient.masterKey));
	        } else if (documentClient.resourceTokens) {
	            return encodeURIComponent(this.getAuthorizationTokenUsingResourceTokens(documentClient.resourceTokens, path, resourceId));
	        }
	    },

	    getAuthorizationTokenUsingMasterKey: function (verb, resourceId, resourceType, headers, masterKey) {
	        var key = new Buffer(masterKey, "base64");

	        var text = (verb || "").toLowerCase() + "\n" +
	            (resourceType || "").toLowerCase() + "\n" +
	            (resourceId || "") + "\n" +
	            (headers["x-ms-date"] || "").toLowerCase() + "\n" +
	            (headers["date"] || "").toLowerCase() + "\n";

	        var body = new Buffer(text, "utf8");

	        var signature = crypto.createHmac("sha256", key).update(body).digest("base64");

	        var MasterToken = "master";

	        var TokenVersion = "1.0";

	        return "type=" + MasterToken + "&ver=" + TokenVersion + "&sig=" + signature;
	    },

	    getAuthorizationTokenUsingResourceTokens: function (resourceTokens, path, resourceId) {
	        if (resourceTokens && Object.keys(resourceTokens).length > 0) {
	            // For database account access(through getDatabaseAccount API), path and resourceId are "", 
	            // so in this case we return the first token to be used for creating the auth header as the service will accept any token in this case
	            if (!path && !resourceId) {
	                return resourceTokens[Object.keys(resourceTokens)[0]];
	            }

	            if (resourceId && resourceTokens[resourceId]) {
	                return resourceTokens[resourceId];
	            }

	            //minimum valid path /dbs
	            if (!path || path.length < 4) {
	                return null;
	            }

	            //remove '/' from left and right of path
	            path = path[0] == '/' ? path.substring(1) : path;
	            path = path[path.length - 1] == '/' ? path.substring(0, path.length - 1) : path;

	            var pathSegments = (path && path.split("/")) || [];

	            //if it's an incomplete path like /dbs/db1/colls/, start from the paretn resource
	            var index = pathSegments.length % 2 === 0 ? pathSegments.length - 1 : pathSegments.length - 2;
	            for (; index > 0; index -= 2) {
	                var id = decodeURI(pathSegments[index]);
	                if (resourceTokens[id]) {
	                    return resourceTokens[id];
	                }
	            }
	        }
	        return null;
	    }

	};

	if (true) {
	    module.exports = AuthHandler;
	}
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 10 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {var rng = __webpack_require__(11)

	function error () {
	  var m = [].slice.call(arguments).join(' ')
	  throw new Error([
	    m,
	    'we accept pull requests',
	    'http://github.com/dominictarr/crypto-browserify'
	    ].join('\n'))
	}

	exports.createHash = __webpack_require__(13)

	exports.createHmac = __webpack_require__(26)

	exports.randomBytes = function(size, callback) {
	  if (callback && callback.call) {
	    try {
	      callback.call(this, undefined, new Buffer(rng(size)))
	    } catch (err) { callback(err) }
	  } else {
	    return new Buffer(rng(size))
	  }
	}

	function each(a, f) {
	  for(var i in a)
	    f(a[i], i)
	}

	exports.getHashes = function () {
	  return ['sha1', 'sha256', 'sha512', 'md5', 'rmd160']
	}

	var p = __webpack_require__(27)(exports)
	exports.pbkdf2 = p.pbkdf2
	exports.pbkdf2Sync = p.pbkdf2Sync
	__webpack_require__(29)(exports, module.exports);

	// the least I can do is make error messages for the rest of the node.js/crypto api.
	each(['createCredentials'
	, 'createSign'
	, 'createVerify'
	, 'createDiffieHellman'
	], function (name) {
	  exports[name] = function () {
	    error('sorry,', name, 'is not implemented yet')
	  }
	})

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 11 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(global, Buffer) {(function() {
	  var g = ('undefined' === typeof window ? global : window) || {}
	  _crypto = (
	    g.crypto || g.msCrypto || __webpack_require__(12)
	  )
	  module.exports = function(size) {
	    // Modern Browsers
	    if(_crypto.getRandomValues) {
	      var bytes = new Buffer(size); //in browserify, this is an extended Uint8Array
	      /* This will not work in older browsers.
	       * See https://developer.mozilla.org/en-US/docs/Web/API/window.crypto.getRandomValues
	       */
	    
	      _crypto.getRandomValues(bytes);
	      return bytes;
	    }
	    else if (_crypto.randomBytes) {
	      return _crypto.randomBytes(size)
	    }
	    else
	      throw new Error(
	        'secure random number generation not supported by this browser\n'+
	        'use chrome, FireFox or Internet Explorer 11'
	      )
	  }
	}())

	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }()), __webpack_require__(5).Buffer))

/***/ }),
/* 12 */
/***/ (function(module, exports) {

	/* (ignored) */

/***/ }),
/* 13 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {var createHash = __webpack_require__(14)

	var md5 = toConstructor(__webpack_require__(23))
	var rmd160 = toConstructor(__webpack_require__(25))

	function toConstructor (fn) {
	  return function () {
	    var buffers = []
	    var m= {
	      update: function (data, enc) {
	        if(!Buffer.isBuffer(data)) data = new Buffer(data, enc)
	        buffers.push(data)
	        return this
	      },
	      digest: function (enc) {
	        var buf = Buffer.concat(buffers)
	        var r = fn(buf)
	        buffers = null
	        return enc ? r.toString(enc) : r
	      }
	    }
	    return m
	  }
	}

	module.exports = function (alg) {
	  if('md5' === alg) return new md5()
	  if('rmd160' === alg) return new rmd160()
	  return createHash(alg)
	}

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 14 */
/***/ (function(module, exports, __webpack_require__) {

	var exports = module.exports = function (alg) {
	  var Alg = exports[alg]
	  if(!Alg) throw new Error(alg + ' is not supported (we accept pull requests)')
	  return new Alg()
	}

	var Buffer = __webpack_require__(5).Buffer
	var Hash   = __webpack_require__(15)(Buffer)

	exports.sha1 = __webpack_require__(16)(Buffer, Hash)
	exports.sha256 = __webpack_require__(21)(Buffer, Hash)
	exports.sha512 = __webpack_require__(22)(Buffer, Hash)


/***/ }),
/* 15 */
/***/ (function(module, exports) {

	module.exports = function (Buffer) {

	  //prototype class for hash functions
	  function Hash (blockSize, finalSize) {
	    this._block = new Buffer(blockSize) //new Uint32Array(blockSize/4)
	    this._finalSize = finalSize
	    this._blockSize = blockSize
	    this._len = 0
	    this._s = 0
	  }

	  Hash.prototype.init = function () {
	    this._s = 0
	    this._len = 0
	  }

	  Hash.prototype.update = function (data, enc) {
	    if ("string" === typeof data) {
	      enc = enc || "utf8"
	      data = new Buffer(data, enc)
	    }

	    var l = this._len += data.length
	    var s = this._s = (this._s || 0)
	    var f = 0
	    var buffer = this._block

	    while (s < l) {
	      var t = Math.min(data.length, f + this._blockSize - (s % this._blockSize))
	      var ch = (t - f)

	      for (var i = 0; i < ch; i++) {
	        buffer[(s % this._blockSize) + i] = data[i + f]
	      }

	      s += ch
	      f += ch

	      if ((s % this._blockSize) === 0) {
	        this._update(buffer)
	      }
	    }
	    this._s = s

	    return this
	  }

	  Hash.prototype.digest = function (enc) {
	    // Suppose the length of the message M, in bits, is l
	    var l = this._len * 8

	    // Append the bit 1 to the end of the message
	    this._block[this._len % this._blockSize] = 0x80

	    // and then k zero bits, where k is the smallest non-negative solution to the equation (l + 1 + k) === finalSize mod blockSize
	    this._block.fill(0, this._len % this._blockSize + 1)

	    if (l % (this._blockSize * 8) >= this._finalSize * 8) {
	      this._update(this._block)
	      this._block.fill(0)
	    }

	    // to this append the block which is equal to the number l written in binary
	    // TODO: handle case where l is > Math.pow(2, 29)
	    this._block.writeInt32BE(l, this._blockSize - 4)

	    var hash = this._update(this._block) || this._hash()

	    return enc ? hash.toString(enc) : hash
	  }

	  Hash.prototype._update = function () {
	    throw new Error('_update must be implemented by subclass')
	  }

	  return Hash
	}


/***/ }),
/* 16 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	 * A JavaScript implementation of the Secure Hash Algorithm, SHA-1, as defined
	 * in FIPS PUB 180-1
	 * Version 2.1a Copyright Paul Johnston 2000 - 2002.
	 * Other contributors: Greg Holt, Andrew Kepert, Ydnar, Lostinet
	 * Distributed under the BSD License
	 * See http://pajhome.org.uk/crypt/md5 for details.
	 */

	var inherits = __webpack_require__(17).inherits

	module.exports = function (Buffer, Hash) {

	  var A = 0|0
	  var B = 4|0
	  var C = 8|0
	  var D = 12|0
	  var E = 16|0

	  var W = new (typeof Int32Array === 'undefined' ? Array : Int32Array)(80)

	  var POOL = []

	  function Sha1 () {
	    if(POOL.length)
	      return POOL.pop().init()

	    if(!(this instanceof Sha1)) return new Sha1()
	    this._w = W
	    Hash.call(this, 16*4, 14*4)

	    this._h = null
	    this.init()
	  }

	  inherits(Sha1, Hash)

	  Sha1.prototype.init = function () {
	    this._a = 0x67452301
	    this._b = 0xefcdab89
	    this._c = 0x98badcfe
	    this._d = 0x10325476
	    this._e = 0xc3d2e1f0

	    Hash.prototype.init.call(this)
	    return this
	  }

	  Sha1.prototype._POOL = POOL
	  Sha1.prototype._update = function (X) {

	    var a, b, c, d, e, _a, _b, _c, _d, _e

	    a = _a = this._a
	    b = _b = this._b
	    c = _c = this._c
	    d = _d = this._d
	    e = _e = this._e

	    var w = this._w

	    for(var j = 0; j < 80; j++) {
	      var W = w[j] = j < 16 ? X.readInt32BE(j*4)
	        : rol(w[j - 3] ^ w[j -  8] ^ w[j - 14] ^ w[j - 16], 1)

	      var t = add(
	        add(rol(a, 5), sha1_ft(j, b, c, d)),
	        add(add(e, W), sha1_kt(j))
	      )

	      e = d
	      d = c
	      c = rol(b, 30)
	      b = a
	      a = t
	    }

	    this._a = add(a, _a)
	    this._b = add(b, _b)
	    this._c = add(c, _c)
	    this._d = add(d, _d)
	    this._e = add(e, _e)
	  }

	  Sha1.prototype._hash = function () {
	    if(POOL.length < 100) POOL.push(this)
	    var H = new Buffer(20)
	    //console.log(this._a|0, this._b|0, this._c|0, this._d|0, this._e|0)
	    H.writeInt32BE(this._a|0, A)
	    H.writeInt32BE(this._b|0, B)
	    H.writeInt32BE(this._c|0, C)
	    H.writeInt32BE(this._d|0, D)
	    H.writeInt32BE(this._e|0, E)
	    return H
	  }

	  /*
	   * Perform the appropriate triplet combination function for the current
	   * iteration
	   */
	  function sha1_ft(t, b, c, d) {
	    if(t < 20) return (b & c) | ((~b) & d);
	    if(t < 40) return b ^ c ^ d;
	    if(t < 60) return (b & c) | (b & d) | (c & d);
	    return b ^ c ^ d;
	  }

	  /*
	   * Determine the appropriate additive constant for the current iteration
	   */
	  function sha1_kt(t) {
	    return (t < 20) ?  1518500249 : (t < 40) ?  1859775393 :
	           (t < 60) ? -1894007588 : -899497514;
	  }

	  /*
	   * Add integers, wrapping at 2^32. This uses 16-bit operations internally
	   * to work around bugs in some JS interpreters.
	   * //dominictarr: this is 10 years old, so maybe this can be dropped?)
	   *
	   */
	  function add(x, y) {
	    return (x + y ) | 0
	  //lets see how this goes on testling.
	  //  var lsw = (x & 0xFFFF) + (y & 0xFFFF);
	  //  var msw = (x >> 16) + (y >> 16) + (lsw >> 16);
	  //  return (msw << 16) | (lsw & 0xFFFF);
	  }

	  /*
	   * Bitwise rotate a 32-bit number to the left.
	   */
	  function rol(num, cnt) {
	    return (num << cnt) | (num >>> (32 - cnt));
	  }

	  return Sha1
	}


/***/ }),
/* 17 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(global, process) {// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	var formatRegExp = /%[sdj%]/g;
	exports.format = function(f) {
	  if (!isString(f)) {
	    var objects = [];
	    for (var i = 0; i < arguments.length; i++) {
	      objects.push(inspect(arguments[i]));
	    }
	    return objects.join(' ');
	  }

	  var i = 1;
	  var args = arguments;
	  var len = args.length;
	  var str = String(f).replace(formatRegExp, function(x) {
	    if (x === '%%') return '%';
	    if (i >= len) return x;
	    switch (x) {
	      case '%s': return String(args[i++]);
	      case '%d': return Number(args[i++]);
	      case '%j':
	        try {
	          return JSON.stringify(args[i++]);
	        } catch (_) {
	          return '[Circular]';
	        }
	      default:
	        return x;
	    }
	  });
	  for (var x = args[i]; i < len; x = args[++i]) {
	    if (isNull(x) || !isObject(x)) {
	      str += ' ' + x;
	    } else {
	      str += ' ' + inspect(x);
	    }
	  }
	  return str;
	};


	// Mark that a method should not be used.
	// Returns a modified function which warns once by default.
	// If --no-deprecation is set, then it is a no-op.
	exports.deprecate = function(fn, msg) {
	  // Allow for deprecating things in the process of starting up.
	  if (isUndefined(global.process)) {
	    return function() {
	      return exports.deprecate(fn, msg).apply(this, arguments);
	    };
	  }

	  if (process.noDeprecation === true) {
	    return fn;
	  }

	  var warned = false;
	  function deprecated() {
	    if (!warned) {
	      if (process.throwDeprecation) {
	        throw new Error(msg);
	      } else if (process.traceDeprecation) {
	        console.trace(msg);
	      } else {
	        console.error(msg);
	      }
	      warned = true;
	    }
	    return fn.apply(this, arguments);
	  }

	  return deprecated;
	};


	var debugs = {};
	var debugEnviron;
	exports.debuglog = function(set) {
	  if (isUndefined(debugEnviron))
	    debugEnviron = process.env.NODE_DEBUG || '';
	  set = set.toUpperCase();
	  if (!debugs[set]) {
	    if (new RegExp('\\b' + set + '\\b', 'i').test(debugEnviron)) {
	      var pid = process.pid;
	      debugs[set] = function() {
	        var msg = exports.format.apply(exports, arguments);
	        console.error('%s %d: %s', set, pid, msg);
	      };
	    } else {
	      debugs[set] = function() {};
	    }
	  }
	  return debugs[set];
	};


	/**
	 * Echos the value of a value. Trys to print the value out
	 * in the best way possible given the different types.
	 *
	 * @param {Object} obj The object to print out.
	 * @param {Object} opts Optional options object that alters the output.
	 */
	/* legacy: obj, showHidden, depth, colors*/
	function inspect(obj, opts) {
	  // default options
	  var ctx = {
	    seen: [],
	    stylize: stylizeNoColor
	  };
	  // legacy...
	  if (arguments.length >= 3) ctx.depth = arguments[2];
	  if (arguments.length >= 4) ctx.colors = arguments[3];
	  if (isBoolean(opts)) {
	    // legacy...
	    ctx.showHidden = opts;
	  } else if (opts) {
	    // got an "options" object
	    exports._extend(ctx, opts);
	  }
	  // set default options
	  if (isUndefined(ctx.showHidden)) ctx.showHidden = false;
	  if (isUndefined(ctx.depth)) ctx.depth = 2;
	  if (isUndefined(ctx.colors)) ctx.colors = false;
	  if (isUndefined(ctx.customInspect)) ctx.customInspect = true;
	  if (ctx.colors) ctx.stylize = stylizeWithColor;
	  return formatValue(ctx, obj, ctx.depth);
	}
	exports.inspect = inspect;


	// http://en.wikipedia.org/wiki/ANSI_escape_code#graphics
	inspect.colors = {
	  'bold' : [1, 22],
	  'italic' : [3, 23],
	  'underline' : [4, 24],
	  'inverse' : [7, 27],
	  'white' : [37, 39],
	  'grey' : [90, 39],
	  'black' : [30, 39],
	  'blue' : [34, 39],
	  'cyan' : [36, 39],
	  'green' : [32, 39],
	  'magenta' : [35, 39],
	  'red' : [31, 39],
	  'yellow' : [33, 39]
	};

	// Don't use 'blue' not visible on cmd.exe
	inspect.styles = {
	  'special': 'cyan',
	  'number': 'yellow',
	  'boolean': 'yellow',
	  'undefined': 'grey',
	  'null': 'bold',
	  'string': 'green',
	  'date': 'magenta',
	  // "name": intentionally not styling
	  'regexp': 'red'
	};


	function stylizeWithColor(str, styleType) {
	  var style = inspect.styles[styleType];

	  if (style) {
	    return '\u001b[' + inspect.colors[style][0] + 'm' + str +
	           '\u001b[' + inspect.colors[style][1] + 'm';
	  } else {
	    return str;
	  }
	}


	function stylizeNoColor(str, styleType) {
	  return str;
	}


	function arrayToHash(array) {
	  var hash = {};

	  array.forEach(function(val, idx) {
	    hash[val] = true;
	  });

	  return hash;
	}


	function formatValue(ctx, value, recurseTimes) {
	  // Provide a hook for user-specified inspect functions.
	  // Check that value is an object with an inspect function on it
	  if (ctx.customInspect &&
	      value &&
	      isFunction(value.inspect) &&
	      // Filter out the util module, it's inspect function is special
	      value.inspect !== exports.inspect &&
	      // Also filter out any prototype objects using the circular check.
	      !(value.constructor && value.constructor.prototype === value)) {
	    var ret = value.inspect(recurseTimes, ctx);
	    if (!isString(ret)) {
	      ret = formatValue(ctx, ret, recurseTimes);
	    }
	    return ret;
	  }

	  // Primitive types cannot have properties
	  var primitive = formatPrimitive(ctx, value);
	  if (primitive) {
	    return primitive;
	  }

	  // Look up the keys of the object.
	  var keys = Object.keys(value);
	  var visibleKeys = arrayToHash(keys);

	  if (ctx.showHidden) {
	    keys = Object.getOwnPropertyNames(value);
	  }

	  // IE doesn't make error fields non-enumerable
	  // http://msdn.microsoft.com/en-us/library/ie/dww52sbt(v=vs.94).aspx
	  if (isError(value)
	      && (keys.indexOf('message') >= 0 || keys.indexOf('description') >= 0)) {
	    return formatError(value);
	  }

	  // Some type of object without properties can be shortcutted.
	  if (keys.length === 0) {
	    if (isFunction(value)) {
	      var name = value.name ? ': ' + value.name : '';
	      return ctx.stylize('[Function' + name + ']', 'special');
	    }
	    if (isRegExp(value)) {
	      return ctx.stylize(RegExp.prototype.toString.call(value), 'regexp');
	    }
	    if (isDate(value)) {
	      return ctx.stylize(Date.prototype.toString.call(value), 'date');
	    }
	    if (isError(value)) {
	      return formatError(value);
	    }
	  }

	  var base = '', array = false, braces = ['{', '}'];

	  // Make Array say that they are Array
	  if (isArray(value)) {
	    array = true;
	    braces = ['[', ']'];
	  }

	  // Make functions say that they are functions
	  if (isFunction(value)) {
	    var n = value.name ? ': ' + value.name : '';
	    base = ' [Function' + n + ']';
	  }

	  // Make RegExps say that they are RegExps
	  if (isRegExp(value)) {
	    base = ' ' + RegExp.prototype.toString.call(value);
	  }

	  // Make dates with properties first say the date
	  if (isDate(value)) {
	    base = ' ' + Date.prototype.toUTCString.call(value);
	  }

	  // Make error with message first say the error
	  if (isError(value)) {
	    base = ' ' + formatError(value);
	  }

	  if (keys.length === 0 && (!array || value.length == 0)) {
	    return braces[0] + base + braces[1];
	  }

	  if (recurseTimes < 0) {
	    if (isRegExp(value)) {
	      return ctx.stylize(RegExp.prototype.toString.call(value), 'regexp');
	    } else {
	      return ctx.stylize('[Object]', 'special');
	    }
	  }

	  ctx.seen.push(value);

	  var output;
	  if (array) {
	    output = formatArray(ctx, value, recurseTimes, visibleKeys, keys);
	  } else {
	    output = keys.map(function(key) {
	      return formatProperty(ctx, value, recurseTimes, visibleKeys, key, array);
	    });
	  }

	  ctx.seen.pop();

	  return reduceToSingleString(output, base, braces);
	}


	function formatPrimitive(ctx, value) {
	  if (isUndefined(value))
	    return ctx.stylize('undefined', 'undefined');
	  if (isString(value)) {
	    var simple = '\'' + JSON.stringify(value).replace(/^"|"$/g, '')
	                                             .replace(/'/g, "\\'")
	                                             .replace(/\\"/g, '"') + '\'';
	    return ctx.stylize(simple, 'string');
	  }
	  if (isNumber(value))
	    return ctx.stylize('' + value, 'number');
	  if (isBoolean(value))
	    return ctx.stylize('' + value, 'boolean');
	  // For some reason typeof null is "object", so special case here.
	  if (isNull(value))
	    return ctx.stylize('null', 'null');
	}


	function formatError(value) {
	  return '[' + Error.prototype.toString.call(value) + ']';
	}


	function formatArray(ctx, value, recurseTimes, visibleKeys, keys) {
	  var output = [];
	  for (var i = 0, l = value.length; i < l; ++i) {
	    if (hasOwnProperty(value, String(i))) {
	      output.push(formatProperty(ctx, value, recurseTimes, visibleKeys,
	          String(i), true));
	    } else {
	      output.push('');
	    }
	  }
	  keys.forEach(function(key) {
	    if (!key.match(/^\d+$/)) {
	      output.push(formatProperty(ctx, value, recurseTimes, visibleKeys,
	          key, true));
	    }
	  });
	  return output;
	}


	function formatProperty(ctx, value, recurseTimes, visibleKeys, key, array) {
	  var name, str, desc;
	  desc = Object.getOwnPropertyDescriptor(value, key) || { value: value[key] };
	  if (desc.get) {
	    if (desc.set) {
	      str = ctx.stylize('[Getter/Setter]', 'special');
	    } else {
	      str = ctx.stylize('[Getter]', 'special');
	    }
	  } else {
	    if (desc.set) {
	      str = ctx.stylize('[Setter]', 'special');
	    }
	  }
	  if (!hasOwnProperty(visibleKeys, key)) {
	    name = '[' + key + ']';
	  }
	  if (!str) {
	    if (ctx.seen.indexOf(desc.value) < 0) {
	      if (isNull(recurseTimes)) {
	        str = formatValue(ctx, desc.value, null);
	      } else {
	        str = formatValue(ctx, desc.value, recurseTimes - 1);
	      }
	      if (str.indexOf('\n') > -1) {
	        if (array) {
	          str = str.split('\n').map(function(line) {
	            return '  ' + line;
	          }).join('\n').substr(2);
	        } else {
	          str = '\n' + str.split('\n').map(function(line) {
	            return '   ' + line;
	          }).join('\n');
	        }
	      }
	    } else {
	      str = ctx.stylize('[Circular]', 'special');
	    }
	  }
	  if (isUndefined(name)) {
	    if (array && key.match(/^\d+$/)) {
	      return str;
	    }
	    name = JSON.stringify('' + key);
	    if (name.match(/^"([a-zA-Z_][a-zA-Z_0-9]*)"$/)) {
	      name = name.substr(1, name.length - 2);
	      name = ctx.stylize(name, 'name');
	    } else {
	      name = name.replace(/'/g, "\\'")
	                 .replace(/\\"/g, '"')
	                 .replace(/(^"|"$)/g, "'");
	      name = ctx.stylize(name, 'string');
	    }
	  }

	  return name + ': ' + str;
	}


	function reduceToSingleString(output, base, braces) {
	  var numLinesEst = 0;
	  var length = output.reduce(function(prev, cur) {
	    numLinesEst++;
	    if (cur.indexOf('\n') >= 0) numLinesEst++;
	    return prev + cur.replace(/\u001b\[\d\d?m/g, '').length + 1;
	  }, 0);

	  if (length > 60) {
	    return braces[0] +
	           (base === '' ? '' : base + '\n ') +
	           ' ' +
	           output.join(',\n  ') +
	           ' ' +
	           braces[1];
	  }

	  return braces[0] + base + ' ' + output.join(', ') + ' ' + braces[1];
	}


	// NOTE: These type checking functions intentionally don't use `instanceof`
	// because it is fragile and can be easily faked with `Object.create()`.
	function isArray(ar) {
	  return Array.isArray(ar);
	}
	exports.isArray = isArray;

	function isBoolean(arg) {
	  return typeof arg === 'boolean';
	}
	exports.isBoolean = isBoolean;

	function isNull(arg) {
	  return arg === null;
	}
	exports.isNull = isNull;

	function isNullOrUndefined(arg) {
	  return arg == null;
	}
	exports.isNullOrUndefined = isNullOrUndefined;

	function isNumber(arg) {
	  return typeof arg === 'number';
	}
	exports.isNumber = isNumber;

	function isString(arg) {
	  return typeof arg === 'string';
	}
	exports.isString = isString;

	function isSymbol(arg) {
	  return typeof arg === 'symbol';
	}
	exports.isSymbol = isSymbol;

	function isUndefined(arg) {
	  return arg === void 0;
	}
	exports.isUndefined = isUndefined;

	function isRegExp(re) {
	  return isObject(re) && objectToString(re) === '[object RegExp]';
	}
	exports.isRegExp = isRegExp;

	function isObject(arg) {
	  return typeof arg === 'object' && arg !== null;
	}
	exports.isObject = isObject;

	function isDate(d) {
	  return isObject(d) && objectToString(d) === '[object Date]';
	}
	exports.isDate = isDate;

	function isError(e) {
	  return isObject(e) &&
	      (objectToString(e) === '[object Error]' || e instanceof Error);
	}
	exports.isError = isError;

	function isFunction(arg) {
	  return typeof arg === 'function';
	}
	exports.isFunction = isFunction;

	function isPrimitive(arg) {
	  return arg === null ||
	         typeof arg === 'boolean' ||
	         typeof arg === 'number' ||
	         typeof arg === 'string' ||
	         typeof arg === 'symbol' ||  // ES6 symbol
	         typeof arg === 'undefined';
	}
	exports.isPrimitive = isPrimitive;

	exports.isBuffer = __webpack_require__(19);

	function objectToString(o) {
	  return Object.prototype.toString.call(o);
	}


	function pad(n) {
	  return n < 10 ? '0' + n.toString(10) : n.toString(10);
	}


	var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
	              'Oct', 'Nov', 'Dec'];

	// 26 Feb 16:19:34
	function timestamp() {
	  var d = new Date();
	  var time = [pad(d.getHours()),
	              pad(d.getMinutes()),
	              pad(d.getSeconds())].join(':');
	  return [d.getDate(), months[d.getMonth()], time].join(' ');
	}


	// log is just a thin wrapper to console.log that prepends a timestamp
	exports.log = function() {
	  console.log('%s - %s', timestamp(), exports.format.apply(exports, arguments));
	};


	/**
	 * Inherit the prototype methods from one constructor into another.
	 *
	 * The Function.prototype.inherits from lang.js rewritten as a standalone
	 * function (not on Function.prototype). NOTE: If this file is to be loaded
	 * during bootstrapping this function needs to be rewritten using some native
	 * functions as prototype setup using normal JavaScript does not work as
	 * expected during bootstrapping (see mirror.js in r114903).
	 *
	 * @param {function} ctor Constructor function which needs to inherit the
	 *     prototype.
	 * @param {function} superCtor Constructor function to inherit prototype from.
	 */
	exports.inherits = __webpack_require__(20);

	exports._extend = function(origin, add) {
	  // Don't do anything if add isn't an object
	  if (!add || !isObject(add)) return origin;

	  var keys = Object.keys(add);
	  var i = keys.length;
	  while (i--) {
	    origin[keys[i]] = add[keys[i]];
	  }
	  return origin;
	};

	function hasOwnProperty(obj, prop) {
	  return Object.prototype.hasOwnProperty.call(obj, prop);
	}

	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }()), __webpack_require__(18)))

/***/ }),
/* 18 */
/***/ (function(module, exports) {

	// shim for using process in browser
	var process = module.exports = {};

	// cached from whatever global is present so that test runners that stub it
	// don't break things.  But we need to wrap it in a try catch in case it is
	// wrapped in strict mode code which doesn't define any globals.  It's inside a
	// function because try/catches deoptimize in certain engines.

	var cachedSetTimeout;
	var cachedClearTimeout;

	function defaultSetTimout() {
	    throw new Error('setTimeout has not been defined');
	}
	function defaultClearTimeout () {
	    throw new Error('clearTimeout has not been defined');
	}
	(function () {
	    try {
	        if (typeof setTimeout === 'function') {
	            cachedSetTimeout = setTimeout;
	        } else {
	            cachedSetTimeout = defaultSetTimout;
	        }
	    } catch (e) {
	        cachedSetTimeout = defaultSetTimout;
	    }
	    try {
	        if (typeof clearTimeout === 'function') {
	            cachedClearTimeout = clearTimeout;
	        } else {
	            cachedClearTimeout = defaultClearTimeout;
	        }
	    } catch (e) {
	        cachedClearTimeout = defaultClearTimeout;
	    }
	} ())
	function runTimeout(fun) {
	    if (cachedSetTimeout === setTimeout) {
	        //normal enviroments in sane situations
	        return setTimeout(fun, 0);
	    }
	    // if setTimeout wasn't available but was latter defined
	    if ((cachedSetTimeout === defaultSetTimout || !cachedSetTimeout) && setTimeout) {
	        cachedSetTimeout = setTimeout;
	        return setTimeout(fun, 0);
	    }
	    try {
	        // when when somebody has screwed with setTimeout but no I.E. maddness
	        return cachedSetTimeout(fun, 0);
	    } catch(e){
	        try {
	            // When we are in I.E. but the script has been evaled so I.E. doesn't trust the global object when called normally
	            return cachedSetTimeout.call(null, fun, 0);
	        } catch(e){
	            // same as above but when it's a version of I.E. that must have the global object for 'this', hopfully our context correct otherwise it will throw a global error
	            return cachedSetTimeout.call(this, fun, 0);
	        }
	    }


	}
	function runClearTimeout(marker) {
	    if (cachedClearTimeout === clearTimeout) {
	        //normal enviroments in sane situations
	        return clearTimeout(marker);
	    }
	    // if clearTimeout wasn't available but was latter defined
	    if ((cachedClearTimeout === defaultClearTimeout || !cachedClearTimeout) && clearTimeout) {
	        cachedClearTimeout = clearTimeout;
	        return clearTimeout(marker);
	    }
	    try {
	        // when when somebody has screwed with setTimeout but no I.E. maddness
	        return cachedClearTimeout(marker);
	    } catch (e){
	        try {
	            // When we are in I.E. but the script has been evaled so I.E. doesn't  trust the global object when called normally
	            return cachedClearTimeout.call(null, marker);
	        } catch (e){
	            // same as above but when it's a version of I.E. that must have the global object for 'this', hopfully our context correct otherwise it will throw a global error.
	            // Some versions of I.E. have different rules for clearTimeout vs setTimeout
	            return cachedClearTimeout.call(this, marker);
	        }
	    }



	}
	var queue = [];
	var draining = false;
	var currentQueue;
	var queueIndex = -1;

	function cleanUpNextTick() {
	    if (!draining || !currentQueue) {
	        return;
	    }
	    draining = false;
	    if (currentQueue.length) {
	        queue = currentQueue.concat(queue);
	    } else {
	        queueIndex = -1;
	    }
	    if (queue.length) {
	        drainQueue();
	    }
	}

	function drainQueue() {
	    if (draining) {
	        return;
	    }
	    var timeout = runTimeout(cleanUpNextTick);
	    draining = true;

	    var len = queue.length;
	    while(len) {
	        currentQueue = queue;
	        queue = [];
	        while (++queueIndex < len) {
	            if (currentQueue) {
	                currentQueue[queueIndex].run();
	            }
	        }
	        queueIndex = -1;
	        len = queue.length;
	    }
	    currentQueue = null;
	    draining = false;
	    runClearTimeout(timeout);
	}

	process.nextTick = function (fun) {
	    var args = new Array(arguments.length - 1);
	    if (arguments.length > 1) {
	        for (var i = 1; i < arguments.length; i++) {
	            args[i - 1] = arguments[i];
	        }
	    }
	    queue.push(new Item(fun, args));
	    if (queue.length === 1 && !draining) {
	        runTimeout(drainQueue);
	    }
	};

	// v8 likes predictible objects
	function Item(fun, array) {
	    this.fun = fun;
	    this.array = array;
	}
	Item.prototype.run = function () {
	    this.fun.apply(null, this.array);
	};
	process.title = 'browser';
	process.browser = true;
	process.env = {};
	process.argv = [];
	process.version = ''; // empty string to avoid regexp issues
	process.versions = {};

	function noop() {}

	process.on = noop;
	process.addListener = noop;
	process.once = noop;
	process.off = noop;
	process.removeListener = noop;
	process.removeAllListeners = noop;
	process.emit = noop;
	process.prependListener = noop;
	process.prependOnceListener = noop;

	process.listeners = function (name) { return [] }

	process.binding = function (name) {
	    throw new Error('process.binding is not supported');
	};

	process.cwd = function () { return '/' };
	process.chdir = function (dir) {
	    throw new Error('process.chdir is not supported');
	};
	process.umask = function() { return 0; };


/***/ }),
/* 19 */
/***/ (function(module, exports) {

	module.exports = function isBuffer(arg) {
	  return arg && typeof arg === 'object'
	    && typeof arg.copy === 'function'
	    && typeof arg.fill === 'function'
	    && typeof arg.readUInt8 === 'function';
	}

/***/ }),
/* 20 */
/***/ (function(module, exports) {

	if (typeof Object.create === 'function') {
	  // implementation from standard node.js 'util' module
	  module.exports = function inherits(ctor, superCtor) {
	    ctor.super_ = superCtor
	    ctor.prototype = Object.create(superCtor.prototype, {
	      constructor: {
	        value: ctor,
	        enumerable: false,
	        writable: true,
	        configurable: true
	      }
	    });
	  };
	} else {
	  // old school shim for old browsers
	  module.exports = function inherits(ctor, superCtor) {
	    ctor.super_ = superCtor
	    var TempCtor = function () {}
	    TempCtor.prototype = superCtor.prototype
	    ctor.prototype = new TempCtor()
	    ctor.prototype.constructor = ctor
	  }
	}


/***/ }),
/* 21 */
/***/ (function(module, exports, __webpack_require__) {

	
	/**
	 * A JavaScript implementation of the Secure Hash Algorithm, SHA-256, as defined
	 * in FIPS 180-2
	 * Version 2.2-beta Copyright Angel Marin, Paul Johnston 2000 - 2009.
	 * Other contributors: Greg Holt, Andrew Kepert, Ydnar, Lostinet
	 *
	 */

	var inherits = __webpack_require__(17).inherits

	module.exports = function (Buffer, Hash) {

	  var K = [
	      0x428A2F98, 0x71374491, 0xB5C0FBCF, 0xE9B5DBA5,
	      0x3956C25B, 0x59F111F1, 0x923F82A4, 0xAB1C5ED5,
	      0xD807AA98, 0x12835B01, 0x243185BE, 0x550C7DC3,
	      0x72BE5D74, 0x80DEB1FE, 0x9BDC06A7, 0xC19BF174,
	      0xE49B69C1, 0xEFBE4786, 0x0FC19DC6, 0x240CA1CC,
	      0x2DE92C6F, 0x4A7484AA, 0x5CB0A9DC, 0x76F988DA,
	      0x983E5152, 0xA831C66D, 0xB00327C8, 0xBF597FC7,
	      0xC6E00BF3, 0xD5A79147, 0x06CA6351, 0x14292967,
	      0x27B70A85, 0x2E1B2138, 0x4D2C6DFC, 0x53380D13,
	      0x650A7354, 0x766A0ABB, 0x81C2C92E, 0x92722C85,
	      0xA2BFE8A1, 0xA81A664B, 0xC24B8B70, 0xC76C51A3,
	      0xD192E819, 0xD6990624, 0xF40E3585, 0x106AA070,
	      0x19A4C116, 0x1E376C08, 0x2748774C, 0x34B0BCB5,
	      0x391C0CB3, 0x4ED8AA4A, 0x5B9CCA4F, 0x682E6FF3,
	      0x748F82EE, 0x78A5636F, 0x84C87814, 0x8CC70208,
	      0x90BEFFFA, 0xA4506CEB, 0xBEF9A3F7, 0xC67178F2
	    ]

	  var W = new Array(64)

	  function Sha256() {
	    this.init()

	    this._w = W //new Array(64)

	    Hash.call(this, 16*4, 14*4)
	  }

	  inherits(Sha256, Hash)

	  Sha256.prototype.init = function () {

	    this._a = 0x6a09e667|0
	    this._b = 0xbb67ae85|0
	    this._c = 0x3c6ef372|0
	    this._d = 0xa54ff53a|0
	    this._e = 0x510e527f|0
	    this._f = 0x9b05688c|0
	    this._g = 0x1f83d9ab|0
	    this._h = 0x5be0cd19|0

	    this._len = this._s = 0

	    return this
	  }

	  function S (X, n) {
	    return (X >>> n) | (X << (32 - n));
	  }

	  function R (X, n) {
	    return (X >>> n);
	  }

	  function Ch (x, y, z) {
	    return ((x & y) ^ ((~x) & z));
	  }

	  function Maj (x, y, z) {
	    return ((x & y) ^ (x & z) ^ (y & z));
	  }

	  function Sigma0256 (x) {
	    return (S(x, 2) ^ S(x, 13) ^ S(x, 22));
	  }

	  function Sigma1256 (x) {
	    return (S(x, 6) ^ S(x, 11) ^ S(x, 25));
	  }

	  function Gamma0256 (x) {
	    return (S(x, 7) ^ S(x, 18) ^ R(x, 3));
	  }

	  function Gamma1256 (x) {
	    return (S(x, 17) ^ S(x, 19) ^ R(x, 10));
	  }

	  Sha256.prototype._update = function(M) {

	    var W = this._w
	    var a, b, c, d, e, f, g, h
	    var T1, T2

	    a = this._a | 0
	    b = this._b | 0
	    c = this._c | 0
	    d = this._d | 0
	    e = this._e | 0
	    f = this._f | 0
	    g = this._g | 0
	    h = this._h | 0

	    for (var j = 0; j < 64; j++) {
	      var w = W[j] = j < 16
	        ? M.readInt32BE(j * 4)
	        : Gamma1256(W[j - 2]) + W[j - 7] + Gamma0256(W[j - 15]) + W[j - 16]

	      T1 = h + Sigma1256(e) + Ch(e, f, g) + K[j] + w

	      T2 = Sigma0256(a) + Maj(a, b, c);
	      h = g; g = f; f = e; e = d + T1; d = c; c = b; b = a; a = T1 + T2;
	    }

	    this._a = (a + this._a) | 0
	    this._b = (b + this._b) | 0
	    this._c = (c + this._c) | 0
	    this._d = (d + this._d) | 0
	    this._e = (e + this._e) | 0
	    this._f = (f + this._f) | 0
	    this._g = (g + this._g) | 0
	    this._h = (h + this._h) | 0

	  };

	  Sha256.prototype._hash = function () {
	    var H = new Buffer(32)

	    H.writeInt32BE(this._a,  0)
	    H.writeInt32BE(this._b,  4)
	    H.writeInt32BE(this._c,  8)
	    H.writeInt32BE(this._d, 12)
	    H.writeInt32BE(this._e, 16)
	    H.writeInt32BE(this._f, 20)
	    H.writeInt32BE(this._g, 24)
	    H.writeInt32BE(this._h, 28)

	    return H
	  }

	  return Sha256

	}


/***/ }),
/* 22 */
/***/ (function(module, exports, __webpack_require__) {

	var inherits = __webpack_require__(17).inherits

	module.exports = function (Buffer, Hash) {
	  var K = [
	    0x428a2f98, 0xd728ae22, 0x71374491, 0x23ef65cd,
	    0xb5c0fbcf, 0xec4d3b2f, 0xe9b5dba5, 0x8189dbbc,
	    0x3956c25b, 0xf348b538, 0x59f111f1, 0xb605d019,
	    0x923f82a4, 0xaf194f9b, 0xab1c5ed5, 0xda6d8118,
	    0xd807aa98, 0xa3030242, 0x12835b01, 0x45706fbe,
	    0x243185be, 0x4ee4b28c, 0x550c7dc3, 0xd5ffb4e2,
	    0x72be5d74, 0xf27b896f, 0x80deb1fe, 0x3b1696b1,
	    0x9bdc06a7, 0x25c71235, 0xc19bf174, 0xcf692694,
	    0xe49b69c1, 0x9ef14ad2, 0xefbe4786, 0x384f25e3,
	    0x0fc19dc6, 0x8b8cd5b5, 0x240ca1cc, 0x77ac9c65,
	    0x2de92c6f, 0x592b0275, 0x4a7484aa, 0x6ea6e483,
	    0x5cb0a9dc, 0xbd41fbd4, 0x76f988da, 0x831153b5,
	    0x983e5152, 0xee66dfab, 0xa831c66d, 0x2db43210,
	    0xb00327c8, 0x98fb213f, 0xbf597fc7, 0xbeef0ee4,
	    0xc6e00bf3, 0x3da88fc2, 0xd5a79147, 0x930aa725,
	    0x06ca6351, 0xe003826f, 0x14292967, 0x0a0e6e70,
	    0x27b70a85, 0x46d22ffc, 0x2e1b2138, 0x5c26c926,
	    0x4d2c6dfc, 0x5ac42aed, 0x53380d13, 0x9d95b3df,
	    0x650a7354, 0x8baf63de, 0x766a0abb, 0x3c77b2a8,
	    0x81c2c92e, 0x47edaee6, 0x92722c85, 0x1482353b,
	    0xa2bfe8a1, 0x4cf10364, 0xa81a664b, 0xbc423001,
	    0xc24b8b70, 0xd0f89791, 0xc76c51a3, 0x0654be30,
	    0xd192e819, 0xd6ef5218, 0xd6990624, 0x5565a910,
	    0xf40e3585, 0x5771202a, 0x106aa070, 0x32bbd1b8,
	    0x19a4c116, 0xb8d2d0c8, 0x1e376c08, 0x5141ab53,
	    0x2748774c, 0xdf8eeb99, 0x34b0bcb5, 0xe19b48a8,
	    0x391c0cb3, 0xc5c95a63, 0x4ed8aa4a, 0xe3418acb,
	    0x5b9cca4f, 0x7763e373, 0x682e6ff3, 0xd6b2b8a3,
	    0x748f82ee, 0x5defb2fc, 0x78a5636f, 0x43172f60,
	    0x84c87814, 0xa1f0ab72, 0x8cc70208, 0x1a6439ec,
	    0x90befffa, 0x23631e28, 0xa4506ceb, 0xde82bde9,
	    0xbef9a3f7, 0xb2c67915, 0xc67178f2, 0xe372532b,
	    0xca273ece, 0xea26619c, 0xd186b8c7, 0x21c0c207,
	    0xeada7dd6, 0xcde0eb1e, 0xf57d4f7f, 0xee6ed178,
	    0x06f067aa, 0x72176fba, 0x0a637dc5, 0xa2c898a6,
	    0x113f9804, 0xbef90dae, 0x1b710b35, 0x131c471b,
	    0x28db77f5, 0x23047d84, 0x32caab7b, 0x40c72493,
	    0x3c9ebe0a, 0x15c9bebc, 0x431d67c4, 0x9c100d4c,
	    0x4cc5d4be, 0xcb3e42b6, 0x597f299c, 0xfc657e2a,
	    0x5fcb6fab, 0x3ad6faec, 0x6c44198c, 0x4a475817
	  ]

	  var W = new Array(160)

	  function Sha512() {
	    this.init()
	    this._w = W

	    Hash.call(this, 128, 112)
	  }

	  inherits(Sha512, Hash)

	  Sha512.prototype.init = function () {

	    this._a = 0x6a09e667|0
	    this._b = 0xbb67ae85|0
	    this._c = 0x3c6ef372|0
	    this._d = 0xa54ff53a|0
	    this._e = 0x510e527f|0
	    this._f = 0x9b05688c|0
	    this._g = 0x1f83d9ab|0
	    this._h = 0x5be0cd19|0

	    this._al = 0xf3bcc908|0
	    this._bl = 0x84caa73b|0
	    this._cl = 0xfe94f82b|0
	    this._dl = 0x5f1d36f1|0
	    this._el = 0xade682d1|0
	    this._fl = 0x2b3e6c1f|0
	    this._gl = 0xfb41bd6b|0
	    this._hl = 0x137e2179|0

	    this._len = this._s = 0

	    return this
	  }

	  function S (X, Xl, n) {
	    return (X >>> n) | (Xl << (32 - n))
	  }

	  function Ch (x, y, z) {
	    return ((x & y) ^ ((~x) & z));
	  }

	  function Maj (x, y, z) {
	    return ((x & y) ^ (x & z) ^ (y & z));
	  }

	  Sha512.prototype._update = function(M) {

	    var W = this._w
	    var a, b, c, d, e, f, g, h
	    var al, bl, cl, dl, el, fl, gl, hl

	    a = this._a | 0
	    b = this._b | 0
	    c = this._c | 0
	    d = this._d | 0
	    e = this._e | 0
	    f = this._f | 0
	    g = this._g | 0
	    h = this._h | 0

	    al = this._al | 0
	    bl = this._bl | 0
	    cl = this._cl | 0
	    dl = this._dl | 0
	    el = this._el | 0
	    fl = this._fl | 0
	    gl = this._gl | 0
	    hl = this._hl | 0

	    for (var i = 0; i < 80; i++) {
	      var j = i * 2

	      var Wi, Wil

	      if (i < 16) {
	        Wi = W[j] = M.readInt32BE(j * 4)
	        Wil = W[j + 1] = M.readInt32BE(j * 4 + 4)

	      } else {
	        var x  = W[j - 15*2]
	        var xl = W[j - 15*2 + 1]
	        var gamma0  = S(x, xl, 1) ^ S(x, xl, 8) ^ (x >>> 7)
	        var gamma0l = S(xl, x, 1) ^ S(xl, x, 8) ^ S(xl, x, 7)

	        x  = W[j - 2*2]
	        xl = W[j - 2*2 + 1]
	        var gamma1  = S(x, xl, 19) ^ S(xl, x, 29) ^ (x >>> 6)
	        var gamma1l = S(xl, x, 19) ^ S(x, xl, 29) ^ S(xl, x, 6)

	        // W[i] = gamma0 + W[i - 7] + gamma1 + W[i - 16]
	        var Wi7  = W[j - 7*2]
	        var Wi7l = W[j - 7*2 + 1]

	        var Wi16  = W[j - 16*2]
	        var Wi16l = W[j - 16*2 + 1]

	        Wil = gamma0l + Wi7l
	        Wi  = gamma0  + Wi7 + ((Wil >>> 0) < (gamma0l >>> 0) ? 1 : 0)
	        Wil = Wil + gamma1l
	        Wi  = Wi  + gamma1  + ((Wil >>> 0) < (gamma1l >>> 0) ? 1 : 0)
	        Wil = Wil + Wi16l
	        Wi  = Wi  + Wi16 + ((Wil >>> 0) < (Wi16l >>> 0) ? 1 : 0)

	        W[j] = Wi
	        W[j + 1] = Wil
	      }

	      var maj = Maj(a, b, c)
	      var majl = Maj(al, bl, cl)

	      var sigma0h = S(a, al, 28) ^ S(al, a, 2) ^ S(al, a, 7)
	      var sigma0l = S(al, a, 28) ^ S(a, al, 2) ^ S(a, al, 7)
	      var sigma1h = S(e, el, 14) ^ S(e, el, 18) ^ S(el, e, 9)
	      var sigma1l = S(el, e, 14) ^ S(el, e, 18) ^ S(e, el, 9)

	      // t1 = h + sigma1 + ch + K[i] + W[i]
	      var Ki = K[j]
	      var Kil = K[j + 1]

	      var ch = Ch(e, f, g)
	      var chl = Ch(el, fl, gl)

	      var t1l = hl + sigma1l
	      var t1 = h + sigma1h + ((t1l >>> 0) < (hl >>> 0) ? 1 : 0)
	      t1l = t1l + chl
	      t1 = t1 + ch + ((t1l >>> 0) < (chl >>> 0) ? 1 : 0)
	      t1l = t1l + Kil
	      t1 = t1 + Ki + ((t1l >>> 0) < (Kil >>> 0) ? 1 : 0)
	      t1l = t1l + Wil
	      t1 = t1 + Wi + ((t1l >>> 0) < (Wil >>> 0) ? 1 : 0)

	      // t2 = sigma0 + maj
	      var t2l = sigma0l + majl
	      var t2 = sigma0h + maj + ((t2l >>> 0) < (sigma0l >>> 0) ? 1 : 0)

	      h  = g
	      hl = gl
	      g  = f
	      gl = fl
	      f  = e
	      fl = el
	      el = (dl + t1l) | 0
	      e  = (d + t1 + ((el >>> 0) < (dl >>> 0) ? 1 : 0)) | 0
	      d  = c
	      dl = cl
	      c  = b
	      cl = bl
	      b  = a
	      bl = al
	      al = (t1l + t2l) | 0
	      a  = (t1 + t2 + ((al >>> 0) < (t1l >>> 0) ? 1 : 0)) | 0
	    }

	    this._al = (this._al + al) | 0
	    this._bl = (this._bl + bl) | 0
	    this._cl = (this._cl + cl) | 0
	    this._dl = (this._dl + dl) | 0
	    this._el = (this._el + el) | 0
	    this._fl = (this._fl + fl) | 0
	    this._gl = (this._gl + gl) | 0
	    this._hl = (this._hl + hl) | 0

	    this._a = (this._a + a + ((this._al >>> 0) < (al >>> 0) ? 1 : 0)) | 0
	    this._b = (this._b + b + ((this._bl >>> 0) < (bl >>> 0) ? 1 : 0)) | 0
	    this._c = (this._c + c + ((this._cl >>> 0) < (cl >>> 0) ? 1 : 0)) | 0
	    this._d = (this._d + d + ((this._dl >>> 0) < (dl >>> 0) ? 1 : 0)) | 0
	    this._e = (this._e + e + ((this._el >>> 0) < (el >>> 0) ? 1 : 0)) | 0
	    this._f = (this._f + f + ((this._fl >>> 0) < (fl >>> 0) ? 1 : 0)) | 0
	    this._g = (this._g + g + ((this._gl >>> 0) < (gl >>> 0) ? 1 : 0)) | 0
	    this._h = (this._h + h + ((this._hl >>> 0) < (hl >>> 0) ? 1 : 0)) | 0
	  }

	  Sha512.prototype._hash = function () {
	    var H = new Buffer(64)

	    function writeInt64BE(h, l, offset) {
	      H.writeInt32BE(h, offset)
	      H.writeInt32BE(l, offset + 4)
	    }

	    writeInt64BE(this._a, this._al, 0)
	    writeInt64BE(this._b, this._bl, 8)
	    writeInt64BE(this._c, this._cl, 16)
	    writeInt64BE(this._d, this._dl, 24)
	    writeInt64BE(this._e, this._el, 32)
	    writeInt64BE(this._f, this._fl, 40)
	    writeInt64BE(this._g, this._gl, 48)
	    writeInt64BE(this._h, this._hl, 56)

	    return H
	  }

	  return Sha512

	}


/***/ }),
/* 23 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	 * A JavaScript implementation of the RSA Data Security, Inc. MD5 Message
	 * Digest Algorithm, as defined in RFC 1321.
	 * Version 2.1 Copyright (C) Paul Johnston 1999 - 2002.
	 * Other contributors: Greg Holt, Andrew Kepert, Ydnar, Lostinet
	 * Distributed under the BSD License
	 * See http://pajhome.org.uk/crypt/md5 for more info.
	 */

	var helpers = __webpack_require__(24);

	/*
	 * Calculate the MD5 of an array of little-endian words, and a bit length
	 */
	function core_md5(x, len)
	{
	  /* append padding */
	  x[len >> 5] |= 0x80 << ((len) % 32);
	  x[(((len + 64) >>> 9) << 4) + 14] = len;

	  var a =  1732584193;
	  var b = -271733879;
	  var c = -1732584194;
	  var d =  271733878;

	  for(var i = 0; i < x.length; i += 16)
	  {
	    var olda = a;
	    var oldb = b;
	    var oldc = c;
	    var oldd = d;

	    a = md5_ff(a, b, c, d, x[i+ 0], 7 , -680876936);
	    d = md5_ff(d, a, b, c, x[i+ 1], 12, -389564586);
	    c = md5_ff(c, d, a, b, x[i+ 2], 17,  606105819);
	    b = md5_ff(b, c, d, a, x[i+ 3], 22, -1044525330);
	    a = md5_ff(a, b, c, d, x[i+ 4], 7 , -176418897);
	    d = md5_ff(d, a, b, c, x[i+ 5], 12,  1200080426);
	    c = md5_ff(c, d, a, b, x[i+ 6], 17, -1473231341);
	    b = md5_ff(b, c, d, a, x[i+ 7], 22, -45705983);
	    a = md5_ff(a, b, c, d, x[i+ 8], 7 ,  1770035416);
	    d = md5_ff(d, a, b, c, x[i+ 9], 12, -1958414417);
	    c = md5_ff(c, d, a, b, x[i+10], 17, -42063);
	    b = md5_ff(b, c, d, a, x[i+11], 22, -1990404162);
	    a = md5_ff(a, b, c, d, x[i+12], 7 ,  1804603682);
	    d = md5_ff(d, a, b, c, x[i+13], 12, -40341101);
	    c = md5_ff(c, d, a, b, x[i+14], 17, -1502002290);
	    b = md5_ff(b, c, d, a, x[i+15], 22,  1236535329);

	    a = md5_gg(a, b, c, d, x[i+ 1], 5 , -165796510);
	    d = md5_gg(d, a, b, c, x[i+ 6], 9 , -1069501632);
	    c = md5_gg(c, d, a, b, x[i+11], 14,  643717713);
	    b = md5_gg(b, c, d, a, x[i+ 0], 20, -373897302);
	    a = md5_gg(a, b, c, d, x[i+ 5], 5 , -701558691);
	    d = md5_gg(d, a, b, c, x[i+10], 9 ,  38016083);
	    c = md5_gg(c, d, a, b, x[i+15], 14, -660478335);
	    b = md5_gg(b, c, d, a, x[i+ 4], 20, -405537848);
	    a = md5_gg(a, b, c, d, x[i+ 9], 5 ,  568446438);
	    d = md5_gg(d, a, b, c, x[i+14], 9 , -1019803690);
	    c = md5_gg(c, d, a, b, x[i+ 3], 14, -187363961);
	    b = md5_gg(b, c, d, a, x[i+ 8], 20,  1163531501);
	    a = md5_gg(a, b, c, d, x[i+13], 5 , -1444681467);
	    d = md5_gg(d, a, b, c, x[i+ 2], 9 , -51403784);
	    c = md5_gg(c, d, a, b, x[i+ 7], 14,  1735328473);
	    b = md5_gg(b, c, d, a, x[i+12], 20, -1926607734);

	    a = md5_hh(a, b, c, d, x[i+ 5], 4 , -378558);
	    d = md5_hh(d, a, b, c, x[i+ 8], 11, -2022574463);
	    c = md5_hh(c, d, a, b, x[i+11], 16,  1839030562);
	    b = md5_hh(b, c, d, a, x[i+14], 23, -35309556);
	    a = md5_hh(a, b, c, d, x[i+ 1], 4 , -1530992060);
	    d = md5_hh(d, a, b, c, x[i+ 4], 11,  1272893353);
	    c = md5_hh(c, d, a, b, x[i+ 7], 16, -155497632);
	    b = md5_hh(b, c, d, a, x[i+10], 23, -1094730640);
	    a = md5_hh(a, b, c, d, x[i+13], 4 ,  681279174);
	    d = md5_hh(d, a, b, c, x[i+ 0], 11, -358537222);
	    c = md5_hh(c, d, a, b, x[i+ 3], 16, -722521979);
	    b = md5_hh(b, c, d, a, x[i+ 6], 23,  76029189);
	    a = md5_hh(a, b, c, d, x[i+ 9], 4 , -640364487);
	    d = md5_hh(d, a, b, c, x[i+12], 11, -421815835);
	    c = md5_hh(c, d, a, b, x[i+15], 16,  530742520);
	    b = md5_hh(b, c, d, a, x[i+ 2], 23, -995338651);

	    a = md5_ii(a, b, c, d, x[i+ 0], 6 , -198630844);
	    d = md5_ii(d, a, b, c, x[i+ 7], 10,  1126891415);
	    c = md5_ii(c, d, a, b, x[i+14], 15, -1416354905);
	    b = md5_ii(b, c, d, a, x[i+ 5], 21, -57434055);
	    a = md5_ii(a, b, c, d, x[i+12], 6 ,  1700485571);
	    d = md5_ii(d, a, b, c, x[i+ 3], 10, -1894986606);
	    c = md5_ii(c, d, a, b, x[i+10], 15, -1051523);
	    b = md5_ii(b, c, d, a, x[i+ 1], 21, -2054922799);
	    a = md5_ii(a, b, c, d, x[i+ 8], 6 ,  1873313359);
	    d = md5_ii(d, a, b, c, x[i+15], 10, -30611744);
	    c = md5_ii(c, d, a, b, x[i+ 6], 15, -1560198380);
	    b = md5_ii(b, c, d, a, x[i+13], 21,  1309151649);
	    a = md5_ii(a, b, c, d, x[i+ 4], 6 , -145523070);
	    d = md5_ii(d, a, b, c, x[i+11], 10, -1120210379);
	    c = md5_ii(c, d, a, b, x[i+ 2], 15,  718787259);
	    b = md5_ii(b, c, d, a, x[i+ 9], 21, -343485551);

	    a = safe_add(a, olda);
	    b = safe_add(b, oldb);
	    c = safe_add(c, oldc);
	    d = safe_add(d, oldd);
	  }
	  return Array(a, b, c, d);

	}

	/*
	 * These functions implement the four basic operations the algorithm uses.
	 */
	function md5_cmn(q, a, b, x, s, t)
	{
	  return safe_add(bit_rol(safe_add(safe_add(a, q), safe_add(x, t)), s),b);
	}
	function md5_ff(a, b, c, d, x, s, t)
	{
	  return md5_cmn((b & c) | ((~b) & d), a, b, x, s, t);
	}
	function md5_gg(a, b, c, d, x, s, t)
	{
	  return md5_cmn((b & d) | (c & (~d)), a, b, x, s, t);
	}
	function md5_hh(a, b, c, d, x, s, t)
	{
	  return md5_cmn(b ^ c ^ d, a, b, x, s, t);
	}
	function md5_ii(a, b, c, d, x, s, t)
	{
	  return md5_cmn(c ^ (b | (~d)), a, b, x, s, t);
	}

	/*
	 * Add integers, wrapping at 2^32. This uses 16-bit operations internally
	 * to work around bugs in some JS interpreters.
	 */
	function safe_add(x, y)
	{
	  var lsw = (x & 0xFFFF) + (y & 0xFFFF);
	  var msw = (x >> 16) + (y >> 16) + (lsw >> 16);
	  return (msw << 16) | (lsw & 0xFFFF);
	}

	/*
	 * Bitwise rotate a 32-bit number to the left.
	 */
	function bit_rol(num, cnt)
	{
	  return (num << cnt) | (num >>> (32 - cnt));
	}

	module.exports = function md5(buf) {
	  return helpers.hash(buf, core_md5, 16);
	};


/***/ }),
/* 24 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {var intSize = 4;
	var zeroBuffer = new Buffer(intSize); zeroBuffer.fill(0);
	var chrsz = 8;

	function toArray(buf, bigEndian) {
	  if ((buf.length % intSize) !== 0) {
	    var len = buf.length + (intSize - (buf.length % intSize));
	    buf = Buffer.concat([buf, zeroBuffer], len);
	  }

	  var arr = [];
	  var fn = bigEndian ? buf.readInt32BE : buf.readInt32LE;
	  for (var i = 0; i < buf.length; i += intSize) {
	    arr.push(fn.call(buf, i));
	  }
	  return arr;
	}

	function toBuffer(arr, size, bigEndian) {
	  var buf = new Buffer(size);
	  var fn = bigEndian ? buf.writeInt32BE : buf.writeInt32LE;
	  for (var i = 0; i < arr.length; i++) {
	    fn.call(buf, arr[i], i * 4, true);
	  }
	  return buf;
	}

	function hash(buf, fn, hashSize, bigEndian) {
	  if (!Buffer.isBuffer(buf)) buf = new Buffer(buf);
	  var arr = fn(toArray(buf, bigEndian), buf.length * chrsz);
	  return toBuffer(arr, hashSize, bigEndian);
	}

	module.exports = { hash: hash };

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 25 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {
	module.exports = ripemd160



	/*
	CryptoJS v3.1.2
	code.google.com/p/crypto-js
	(c) 2009-2013 by Jeff Mott. All rights reserved.
	code.google.com/p/crypto-js/wiki/License
	*/
	/** @preserve
	(c) 2012 by Cédric Mesnil. All rights reserved.

	Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

	    - Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
	    - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

	THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
	*/

	// Constants table
	var zl = [
	    0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15,
	    7,  4, 13,  1, 10,  6, 15,  3, 12,  0,  9,  5,  2, 14, 11,  8,
	    3, 10, 14,  4,  9, 15,  8,  1,  2,  7,  0,  6, 13, 11,  5, 12,
	    1,  9, 11, 10,  0,  8, 12,  4, 13,  3,  7, 15, 14,  5,  6,  2,
	    4,  0,  5,  9,  7, 12,  2, 10, 14,  1,  3,  8, 11,  6, 15, 13];
	var zr = [
	    5, 14,  7,  0,  9,  2, 11,  4, 13,  6, 15,  8,  1, 10,  3, 12,
	    6, 11,  3,  7,  0, 13,  5, 10, 14, 15,  8, 12,  4,  9,  1,  2,
	    15,  5,  1,  3,  7, 14,  6,  9, 11,  8, 12,  2, 10,  0,  4, 13,
	    8,  6,  4,  1,  3, 11, 15,  0,  5, 12,  2, 13,  9,  7, 10, 14,
	    12, 15, 10,  4,  1,  5,  8,  7,  6,  2, 13, 14,  0,  3,  9, 11];
	var sl = [
	     11, 14, 15, 12,  5,  8,  7,  9, 11, 13, 14, 15,  6,  7,  9,  8,
	    7, 6,   8, 13, 11,  9,  7, 15,  7, 12, 15,  9, 11,  7, 13, 12,
	    11, 13,  6,  7, 14,  9, 13, 15, 14,  8, 13,  6,  5, 12,  7,  5,
	      11, 12, 14, 15, 14, 15,  9,  8,  9, 14,  5,  6,  8,  6,  5, 12,
	    9, 15,  5, 11,  6,  8, 13, 12,  5, 12, 13, 14, 11,  8,  5,  6 ];
	var sr = [
	    8,  9,  9, 11, 13, 15, 15,  5,  7,  7,  8, 11, 14, 14, 12,  6,
	    9, 13, 15,  7, 12,  8,  9, 11,  7,  7, 12,  7,  6, 15, 13, 11,
	    9,  7, 15, 11,  8,  6,  6, 14, 12, 13,  5, 14, 13, 13,  7,  5,
	    15,  5,  8, 11, 14, 14,  6, 14,  6,  9, 12,  9, 12,  5, 15,  8,
	    8,  5, 12,  9, 12,  5, 14,  6,  8, 13,  6,  5, 15, 13, 11, 11 ];

	var hl =  [ 0x00000000, 0x5A827999, 0x6ED9EBA1, 0x8F1BBCDC, 0xA953FD4E];
	var hr =  [ 0x50A28BE6, 0x5C4DD124, 0x6D703EF3, 0x7A6D76E9, 0x00000000];

	var bytesToWords = function (bytes) {
	  var words = [];
	  for (var i = 0, b = 0; i < bytes.length; i++, b += 8) {
	    words[b >>> 5] |= bytes[i] << (24 - b % 32);
	  }
	  return words;
	};

	var wordsToBytes = function (words) {
	  var bytes = [];
	  for (var b = 0; b < words.length * 32; b += 8) {
	    bytes.push((words[b >>> 5] >>> (24 - b % 32)) & 0xFF);
	  }
	  return bytes;
	};

	var processBlock = function (H, M, offset) {

	  // Swap endian
	  for (var i = 0; i < 16; i++) {
	    var offset_i = offset + i;
	    var M_offset_i = M[offset_i];

	    // Swap
	    M[offset_i] = (
	        (((M_offset_i << 8)  | (M_offset_i >>> 24)) & 0x00ff00ff) |
	        (((M_offset_i << 24) | (M_offset_i >>> 8))  & 0xff00ff00)
	    );
	  }

	  // Working variables
	  var al, bl, cl, dl, el;
	  var ar, br, cr, dr, er;

	  ar = al = H[0];
	  br = bl = H[1];
	  cr = cl = H[2];
	  dr = dl = H[3];
	  er = el = H[4];
	  // Computation
	  var t;
	  for (var i = 0; i < 80; i += 1) {
	    t = (al +  M[offset+zl[i]])|0;
	    if (i<16){
	        t +=  f1(bl,cl,dl) + hl[0];
	    } else if (i<32) {
	        t +=  f2(bl,cl,dl) + hl[1];
	    } else if (i<48) {
	        t +=  f3(bl,cl,dl) + hl[2];
	    } else if (i<64) {
	        t +=  f4(bl,cl,dl) + hl[3];
	    } else {// if (i<80) {
	        t +=  f5(bl,cl,dl) + hl[4];
	    }
	    t = t|0;
	    t =  rotl(t,sl[i]);
	    t = (t+el)|0;
	    al = el;
	    el = dl;
	    dl = rotl(cl, 10);
	    cl = bl;
	    bl = t;

	    t = (ar + M[offset+zr[i]])|0;
	    if (i<16){
	        t +=  f5(br,cr,dr) + hr[0];
	    } else if (i<32) {
	        t +=  f4(br,cr,dr) + hr[1];
	    } else if (i<48) {
	        t +=  f3(br,cr,dr) + hr[2];
	    } else if (i<64) {
	        t +=  f2(br,cr,dr) + hr[3];
	    } else {// if (i<80) {
	        t +=  f1(br,cr,dr) + hr[4];
	    }
	    t = t|0;
	    t =  rotl(t,sr[i]) ;
	    t = (t+er)|0;
	    ar = er;
	    er = dr;
	    dr = rotl(cr, 10);
	    cr = br;
	    br = t;
	  }
	  // Intermediate hash value
	  t    = (H[1] + cl + dr)|0;
	  H[1] = (H[2] + dl + er)|0;
	  H[2] = (H[3] + el + ar)|0;
	  H[3] = (H[4] + al + br)|0;
	  H[4] = (H[0] + bl + cr)|0;
	  H[0] =  t;
	};

	function f1(x, y, z) {
	  return ((x) ^ (y) ^ (z));
	}

	function f2(x, y, z) {
	  return (((x)&(y)) | ((~x)&(z)));
	}

	function f3(x, y, z) {
	  return (((x) | (~(y))) ^ (z));
	}

	function f4(x, y, z) {
	  return (((x) & (z)) | ((y)&(~(z))));
	}

	function f5(x, y, z) {
	  return ((x) ^ ((y) |(~(z))));
	}

	function rotl(x,n) {
	  return (x<<n) | (x>>>(32-n));
	}

	function ripemd160(message) {
	  var H = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];

	  if (typeof message == 'string')
	    message = new Buffer(message, 'utf8');

	  var m = bytesToWords(message);

	  var nBitsLeft = message.length * 8;
	  var nBitsTotal = message.length * 8;

	  // Add padding
	  m[nBitsLeft >>> 5] |= 0x80 << (24 - nBitsLeft % 32);
	  m[(((nBitsLeft + 64) >>> 9) << 4) + 14] = (
	      (((nBitsTotal << 8)  | (nBitsTotal >>> 24)) & 0x00ff00ff) |
	      (((nBitsTotal << 24) | (nBitsTotal >>> 8))  & 0xff00ff00)
	  );

	  for (var i=0 ; i<m.length; i += 16) {
	    processBlock(H, m, i);
	  }

	  // Swap endian
	  for (var i = 0; i < 5; i++) {
	      // Shortcut
	    var H_i = H[i];

	    // Swap
	    H[i] = (((H_i << 8)  | (H_i >>> 24)) & 0x00ff00ff) |
	          (((H_i << 24) | (H_i >>> 8))  & 0xff00ff00);
	  }

	  var digestbytes = wordsToBytes(H);
	  return new Buffer(digestbytes);
	}



	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 26 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {var createHash = __webpack_require__(13)

	var zeroBuffer = new Buffer(128)
	zeroBuffer.fill(0)

	module.exports = Hmac

	function Hmac (alg, key) {
	  if(!(this instanceof Hmac)) return new Hmac(alg, key)
	  this._opad = opad
	  this._alg = alg

	  var blocksize = (alg === 'sha512') ? 128 : 64

	  key = this._key = !Buffer.isBuffer(key) ? new Buffer(key) : key

	  if(key.length > blocksize) {
	    key = createHash(alg).update(key).digest()
	  } else if(key.length < blocksize) {
	    key = Buffer.concat([key, zeroBuffer], blocksize)
	  }

	  var ipad = this._ipad = new Buffer(blocksize)
	  var opad = this._opad = new Buffer(blocksize)

	  for(var i = 0; i < blocksize; i++) {
	    ipad[i] = key[i] ^ 0x36
	    opad[i] = key[i] ^ 0x5C
	  }

	  this._hash = createHash(alg).update(ipad)
	}

	Hmac.prototype.update = function (data, enc) {
	  this._hash.update(data, enc)
	  return this
	}

	Hmac.prototype.digest = function (enc) {
	  var h = this._hash.digest()
	  return createHash(this._alg).update(this._opad).update(h).digest(enc)
	}


	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 27 */
/***/ (function(module, exports, __webpack_require__) {

	var pbkdf2Export = __webpack_require__(28)

	module.exports = function (crypto, exports) {
	  exports = exports || {}

	  var exported = pbkdf2Export(crypto)

	  exports.pbkdf2 = exported.pbkdf2
	  exports.pbkdf2Sync = exported.pbkdf2Sync

	  return exports
	}


/***/ }),
/* 28 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {module.exports = function(crypto) {
	  function pbkdf2(password, salt, iterations, keylen, digest, callback) {
	    if ('function' === typeof digest) {
	      callback = digest
	      digest = undefined
	    }

	    if ('function' !== typeof callback)
	      throw new Error('No callback provided to pbkdf2')

	    setTimeout(function() {
	      var result

	      try {
	        result = pbkdf2Sync(password, salt, iterations, keylen, digest)
	      } catch (e) {
	        return callback(e)
	      }

	      callback(undefined, result)
	    })
	  }

	  function pbkdf2Sync(password, salt, iterations, keylen, digest) {
	    if ('number' !== typeof iterations)
	      throw new TypeError('Iterations not a number')

	    if (iterations < 0)
	      throw new TypeError('Bad iterations')

	    if ('number' !== typeof keylen)
	      throw new TypeError('Key length not a number')

	    if (keylen < 0)
	      throw new TypeError('Bad key length')

	    digest = digest || 'sha1'

	    if (!Buffer.isBuffer(password)) password = new Buffer(password)
	    if (!Buffer.isBuffer(salt)) salt = new Buffer(salt)

	    var hLen, l = 1, r, T
	    var DK = new Buffer(keylen)
	    var block1 = new Buffer(salt.length + 4)
	    salt.copy(block1, 0, 0, salt.length)

	    for (var i = 1; i <= l; i++) {
	      block1.writeUInt32BE(i, salt.length)

	      var U = crypto.createHmac(digest, password).update(block1).digest()

	      if (!hLen) {
	        hLen = U.length
	        T = new Buffer(hLen)
	        l = Math.ceil(keylen / hLen)
	        r = keylen - (l - 1) * hLen

	        if (keylen > (Math.pow(2, 32) - 1) * hLen)
	          throw new TypeError('keylen exceeds maximum length')
	      }

	      U.copy(T, 0, 0, hLen)

	      for (var j = 1; j < iterations; j++) {
	        U = crypto.createHmac(digest, password).update(U).digest()

	        for (var k = 0; k < hLen; k++) {
	          T[k] ^= U[k]
	        }
	      }

	      var destPos = (i - 1) * hLen
	      var len = (i == l ? r : hLen)
	      T.copy(DK, destPos, 0, len)
	    }

	    return DK
	  }

	  return {
	    pbkdf2: pbkdf2,
	    pbkdf2Sync: pbkdf2Sync
	  }
	}

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 29 */
/***/ (function(module, exports, __webpack_require__) {

	module.exports = function (crypto, exports) {
	  exports = exports || {};
	  var ciphers = __webpack_require__(30)(crypto);
	  exports.createCipher = ciphers.createCipher;
	  exports.createCipheriv = ciphers.createCipheriv;
	  var deciphers = __webpack_require__(68)(crypto);
	  exports.createDecipher = deciphers.createDecipher;
	  exports.createDecipheriv = deciphers.createDecipheriv;
	  var modes = __webpack_require__(59);
	  function listCiphers () {
	    return Object.keys(modes);
	  }
	  exports.listCiphers = listCiphers;
	};



/***/ }),
/* 30 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {var aes = __webpack_require__(31);
	var Transform = __webpack_require__(32);
	var inherits = __webpack_require__(35);
	var modes = __webpack_require__(59);
	var ebtk = __webpack_require__(60);
	var StreamCipher = __webpack_require__(61);
	inherits(Cipher, Transform);
	function Cipher(mode, key, iv) {
	  if (!(this instanceof Cipher)) {
	    return new Cipher(mode, key, iv);
	  }
	  Transform.call(this);
	  this._cache = new Splitter();
	  this._cipher = new aes.AES(key);
	  this._prev = new Buffer(iv.length);
	  iv.copy(this._prev);
	  this._mode = mode;
	}
	Cipher.prototype._transform = function (data, _, next) {
	  this._cache.add(data);
	  var chunk;
	  var thing;
	  while ((chunk = this._cache.get())) {
	    thing = this._mode.encrypt(this, chunk);
	    this.push(thing);
	  }
	  next();
	};
	Cipher.prototype._flush = function (next) {
	  var chunk = this._cache.flush();
	  this.push(this._mode.encrypt(this, chunk));
	  this._cipher.scrub();
	  next();
	};


	function Splitter() {
	   if (!(this instanceof Splitter)) {
	    return new Splitter();
	  }
	  this.cache = new Buffer('');
	}
	Splitter.prototype.add = function (data) {
	  this.cache = Buffer.concat([this.cache, data]);
	};

	Splitter.prototype.get = function () {
	  if (this.cache.length > 15) {
	    var out = this.cache.slice(0, 16);
	    this.cache = this.cache.slice(16);
	    return out;
	  }
	  return null;
	};
	Splitter.prototype.flush = function () {
	  var len = 16 - this.cache.length;
	  var padBuff = new Buffer(len);

	  var i = -1;
	  while (++i < len) {
	    padBuff.writeUInt8(len, i);
	  }
	  var out = Buffer.concat([this.cache, padBuff]);
	  return out;
	};
	var modelist = {
	  ECB: __webpack_require__(62),
	  CBC: __webpack_require__(63),
	  CFB: __webpack_require__(65),
	  OFB: __webpack_require__(66),
	  CTR: __webpack_require__(67)
	};
	module.exports = function (crypto) {
	  function createCipheriv(suite, password, iv) {
	    var config = modes[suite];
	    if (!config) {
	      throw new TypeError('invalid suite type');
	    }
	    if (typeof iv === 'string') {
	      iv = new Buffer(iv);
	    }
	    if (typeof password === 'string') {
	      password = new Buffer(password);
	    }
	    if (password.length !== config.key/8) {
	      throw new TypeError('invalid key length ' + password.length);
	    }
	    if (iv.length !== config.iv) {
	      throw new TypeError('invalid iv length ' + iv.length);
	    }
	    if (config.type === 'stream') {
	      return new StreamCipher(modelist[config.mode], password, iv);
	    }
	    return new Cipher(modelist[config.mode], password, iv);
	  }
	  function createCipher (suite, password) {
	    var config = modes[suite];
	    if (!config) {
	      throw new TypeError('invalid suite type');
	    }
	    var keys = ebtk(crypto, password, config.key, config.iv);
	    return createCipheriv(suite, keys.key, keys.iv);
	  }
	  return {
	    createCipher: createCipher,
	    createCipheriv: createCipheriv
	  };
	};

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 31 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {var uint_max = Math.pow(2, 32);
	function fixup_uint32(x) {
	    var ret, x_pos;
	    ret = x > uint_max || x < 0 ? (x_pos = Math.abs(x) % uint_max, x < 0 ? uint_max - x_pos : x_pos) : x;
	    return ret;
	}
	function scrub_vec(v) {
	  var i, _i, _ref;
	  for (i = _i = 0, _ref = v.length; 0 <= _ref ? _i < _ref : _i > _ref; i = 0 <= _ref ? ++_i : --_i) {
	    v[i] = 0;
	  }
	  return false;
	}

	function Global() {
	  var i;
	  this.SBOX = [];
	  this.INV_SBOX = [];
	  this.SUB_MIX = (function() {
	    var _i, _results;
	    _results = [];
	    for (i = _i = 0; _i < 4; i = ++_i) {
	      _results.push([]);
	    }
	    return _results;
	  })();
	  this.INV_SUB_MIX = (function() {
	    var _i, _results;
	    _results = [];
	    for (i = _i = 0; _i < 4; i = ++_i) {
	      _results.push([]);
	    }
	    return _results;
	  })();
	  this.init();
	  this.RCON = [0x00, 0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1b, 0x36];
	}

	Global.prototype.init = function() {
	  var d, i, sx, t, x, x2, x4, x8, xi, _i;
	  d = (function() {
	    var _i, _results;
	    _results = [];
	    for (i = _i = 0; _i < 256; i = ++_i) {
	      if (i < 128) {
	        _results.push(i << 1);
	      } else {
	        _results.push((i << 1) ^ 0x11b);
	      }
	    }
	    return _results;
	  })();
	  x = 0;
	  xi = 0;
	  for (i = _i = 0; _i < 256; i = ++_i) {
	    sx = xi ^ (xi << 1) ^ (xi << 2) ^ (xi << 3) ^ (xi << 4);
	    sx = (sx >>> 8) ^ (sx & 0xff) ^ 0x63;
	    this.SBOX[x] = sx;
	    this.INV_SBOX[sx] = x;
	    x2 = d[x];
	    x4 = d[x2];
	    x8 = d[x4];
	    t = (d[sx] * 0x101) ^ (sx * 0x1010100);
	    this.SUB_MIX[0][x] = (t << 24) | (t >>> 8);
	    this.SUB_MIX[1][x] = (t << 16) | (t >>> 16);
	    this.SUB_MIX[2][x] = (t << 8) | (t >>> 24);
	    this.SUB_MIX[3][x] = t;
	    t = (x8 * 0x1010101) ^ (x4 * 0x10001) ^ (x2 * 0x101) ^ (x * 0x1010100);
	    this.INV_SUB_MIX[0][sx] = (t << 24) | (t >>> 8);
	    this.INV_SUB_MIX[1][sx] = (t << 16) | (t >>> 16);
	    this.INV_SUB_MIX[2][sx] = (t << 8) | (t >>> 24);
	    this.INV_SUB_MIX[3][sx] = t;
	    if (x === 0) {
	      x = xi = 1;
	    } else {
	      x = x2 ^ d[d[d[x8 ^ x2]]];
	      xi ^= d[d[xi]];
	    }
	  }
	  return true;
	};

	var G = new Global();


	AES.blockSize = 4 * 4;

	AES.prototype.blockSize = AES.blockSize;

	AES.keySize = 256 / 8;

	AES.prototype.keySize = AES.keySize;

	AES.ivSize = AES.blockSize;

	AES.prototype.ivSize = AES.ivSize;

	 function bufferToArray(buf) {
	  var len = buf.length/4;
	  var out = new Array(len);
	  var i = -1;
	  while (++i < len) {
	    out[i] = buf.readUInt32BE(i * 4);
	  }
	  return out;
	 }
	function AES(key) {
	  this._key = bufferToArray(key);
	  this._doReset();
	}

	AES.prototype._doReset = function() {
	  var invKsRow, keySize, keyWords, ksRow, ksRows, t, _i, _j;
	  keyWords = this._key;
	  keySize = keyWords.length;
	  this._nRounds = keySize + 6;
	  ksRows = (this._nRounds + 1) * 4;
	  this._keySchedule = [];
	  for (ksRow = _i = 0; 0 <= ksRows ? _i < ksRows : _i > ksRows; ksRow = 0 <= ksRows ? ++_i : --_i) {
	    this._keySchedule[ksRow] = ksRow < keySize ? keyWords[ksRow] : (t = this._keySchedule[ksRow - 1], (ksRow % keySize) === 0 ? (t = (t << 8) | (t >>> 24), t = (G.SBOX[t >>> 24] << 24) | (G.SBOX[(t >>> 16) & 0xff] << 16) | (G.SBOX[(t >>> 8) & 0xff] << 8) | G.SBOX[t & 0xff], t ^= G.RCON[(ksRow / keySize) | 0] << 24) : keySize > 6 && ksRow % keySize === 4 ? t = (G.SBOX[t >>> 24] << 24) | (G.SBOX[(t >>> 16) & 0xff] << 16) | (G.SBOX[(t >>> 8) & 0xff] << 8) | G.SBOX[t & 0xff] : void 0, this._keySchedule[ksRow - keySize] ^ t);
	  }
	  this._invKeySchedule = [];
	  for (invKsRow = _j = 0; 0 <= ksRows ? _j < ksRows : _j > ksRows; invKsRow = 0 <= ksRows ? ++_j : --_j) {
	    ksRow = ksRows - invKsRow;
	    t = this._keySchedule[ksRow - (invKsRow % 4 ? 0 : 4)];
	    this._invKeySchedule[invKsRow] = invKsRow < 4 || ksRow <= 4 ? t : G.INV_SUB_MIX[0][G.SBOX[t >>> 24]] ^ G.INV_SUB_MIX[1][G.SBOX[(t >>> 16) & 0xff]] ^ G.INV_SUB_MIX[2][G.SBOX[(t >>> 8) & 0xff]] ^ G.INV_SUB_MIX[3][G.SBOX[t & 0xff]];
	  }
	  return true;
	};

	AES.prototype.encryptBlock = function(M) {
	  M = bufferToArray(new Buffer(M));
	  var out = this._doCryptBlock(M, this._keySchedule, G.SUB_MIX, G.SBOX);
	  var buf = new Buffer(16);
	  buf.writeUInt32BE(out[0], 0);
	  buf.writeUInt32BE(out[1], 4);
	  buf.writeUInt32BE(out[2], 8);
	  buf.writeUInt32BE(out[3], 12);
	  return buf;
	};

	AES.prototype.decryptBlock = function(M) {
	  M = bufferToArray(new Buffer(M));
	  var temp = [M[3], M[1]];
	  M[1] = temp[0];
	  M[3] = temp[1];
	  var out = this._doCryptBlock(M, this._invKeySchedule, G.INV_SUB_MIX, G.INV_SBOX);
	  var buf = new Buffer(16);
	  buf.writeUInt32BE(out[0], 0);
	  buf.writeUInt32BE(out[3], 4);
	  buf.writeUInt32BE(out[2], 8);
	  buf.writeUInt32BE(out[1], 12);
	  return buf;
	};

	AES.prototype.scrub = function() {
	  scrub_vec(this._keySchedule);
	  scrub_vec(this._invKeySchedule);
	  scrub_vec(this._key);
	};

	AES.prototype._doCryptBlock = function(M, keySchedule, SUB_MIX, SBOX) {
	  var ksRow, round, s0, s1, s2, s3, t0, t1, t2, t3, _i, _ref;

	  s0 = M[0] ^ keySchedule[0];
	  s1 = M[1] ^ keySchedule[1];
	  s2 = M[2] ^ keySchedule[2];
	  s3 = M[3] ^ keySchedule[3];
	  ksRow = 4;
	  for (round = _i = 1, _ref = this._nRounds; 1 <= _ref ? _i < _ref : _i > _ref; round = 1 <= _ref ? ++_i : --_i) {
	    t0 = SUB_MIX[0][s0 >>> 24] ^ SUB_MIX[1][(s1 >>> 16) & 0xff] ^ SUB_MIX[2][(s2 >>> 8) & 0xff] ^ SUB_MIX[3][s3 & 0xff] ^ keySchedule[ksRow++];
	    t1 = SUB_MIX[0][s1 >>> 24] ^ SUB_MIX[1][(s2 >>> 16) & 0xff] ^ SUB_MIX[2][(s3 >>> 8) & 0xff] ^ SUB_MIX[3][s0 & 0xff] ^ keySchedule[ksRow++];
	    t2 = SUB_MIX[0][s2 >>> 24] ^ SUB_MIX[1][(s3 >>> 16) & 0xff] ^ SUB_MIX[2][(s0 >>> 8) & 0xff] ^ SUB_MIX[3][s1 & 0xff] ^ keySchedule[ksRow++];
	    t3 = SUB_MIX[0][s3 >>> 24] ^ SUB_MIX[1][(s0 >>> 16) & 0xff] ^ SUB_MIX[2][(s1 >>> 8) & 0xff] ^ SUB_MIX[3][s2 & 0xff] ^ keySchedule[ksRow++];
	    s0 = t0;
	    s1 = t1;
	    s2 = t2;
	    s3 = t3;
	  }
	  t0 = ((SBOX[s0 >>> 24] << 24) | (SBOX[(s1 >>> 16) & 0xff] << 16) | (SBOX[(s2 >>> 8) & 0xff] << 8) | SBOX[s3 & 0xff]) ^ keySchedule[ksRow++];
	  t1 = ((SBOX[s1 >>> 24] << 24) | (SBOX[(s2 >>> 16) & 0xff] << 16) | (SBOX[(s3 >>> 8) & 0xff] << 8) | SBOX[s0 & 0xff]) ^ keySchedule[ksRow++];
	  t2 = ((SBOX[s2 >>> 24] << 24) | (SBOX[(s3 >>> 16) & 0xff] << 16) | (SBOX[(s0 >>> 8) & 0xff] << 8) | SBOX[s1 & 0xff]) ^ keySchedule[ksRow++];
	  t3 = ((SBOX[s3 >>> 24] << 24) | (SBOX[(s0 >>> 16) & 0xff] << 16) | (SBOX[(s1 >>> 8) & 0xff] << 8) | SBOX[s2 & 0xff]) ^ keySchedule[ksRow++];
	  return [
	    fixup_uint32(t0),
	    fixup_uint32(t1),
	    fixup_uint32(t2),
	    fixup_uint32(t3)
	  ];

	};




	  exports.AES = AES;
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 32 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {var Transform = __webpack_require__(33).Transform;
	var inherits = __webpack_require__(35);

	module.exports = CipherBase;
	inherits(CipherBase, Transform);
	function CipherBase() {
	  Transform.call(this);
	}
	CipherBase.prototype.update = function (data, inputEnd, outputEnc) {
	  this.write(data, inputEnd);
	  var outData = new Buffer('');
	  var chunk;
	  while ((chunk = this.read())) {
	    outData = Buffer.concat([outData, chunk]);
	  }
	  if (outputEnc) {
	    outData = outData.toString(outputEnc);
	  }
	  return outData;
	};
	CipherBase.prototype.final = function (outputEnc) {
	  this.end();
	  var outData = new Buffer('');
	  var chunk;
	  while ((chunk = this.read())) {
	    outData = Buffer.concat([outData, chunk]);
	  }
	  if (outputEnc) {
	    outData = outData.toString(outputEnc);
	  }
	  return outData;
	};
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 33 */
/***/ (function(module, exports, __webpack_require__) {

	// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	module.exports = Stream;

	var EE = __webpack_require__(34).EventEmitter;
	var inherits = __webpack_require__(35);

	inherits(Stream, EE);
	Stream.Readable = __webpack_require__(36);
	Stream.Writable = __webpack_require__(55);
	Stream.Duplex = __webpack_require__(56);
	Stream.Transform = __webpack_require__(57);
	Stream.PassThrough = __webpack_require__(58);

	// Backwards-compat with node 0.4.x
	Stream.Stream = Stream;



	// old-style streams.  Note that the pipe method (the only relevant
	// part of this class) is overridden in the Readable class.

	function Stream() {
	  EE.call(this);
	}

	Stream.prototype.pipe = function(dest, options) {
	  var source = this;

	  function ondata(chunk) {
	    if (dest.writable) {
	      if (false === dest.write(chunk) && source.pause) {
	        source.pause();
	      }
	    }
	  }

	  source.on('data', ondata);

	  function ondrain() {
	    if (source.readable && source.resume) {
	      source.resume();
	    }
	  }

	  dest.on('drain', ondrain);

	  // If the 'end' option is not supplied, dest.end() will be called when
	  // source gets the 'end' or 'close' events.  Only dest.end() once.
	  if (!dest._isStdio && (!options || options.end !== false)) {
	    source.on('end', onend);
	    source.on('close', onclose);
	  }

	  var didOnEnd = false;
	  function onend() {
	    if (didOnEnd) return;
	    didOnEnd = true;

	    dest.end();
	  }


	  function onclose() {
	    if (didOnEnd) return;
	    didOnEnd = true;

	    if (typeof dest.destroy === 'function') dest.destroy();
	  }

	  // don't leave dangling pipes when there are errors.
	  function onerror(er) {
	    cleanup();
	    if (EE.listenerCount(this, 'error') === 0) {
	      throw er; // Unhandled stream error in pipe.
	    }
	  }

	  source.on('error', onerror);
	  dest.on('error', onerror);

	  // remove all the event listeners that were added.
	  function cleanup() {
	    source.removeListener('data', ondata);
	    dest.removeListener('drain', ondrain);

	    source.removeListener('end', onend);
	    source.removeListener('close', onclose);

	    source.removeListener('error', onerror);
	    dest.removeListener('error', onerror);

	    source.removeListener('end', cleanup);
	    source.removeListener('close', cleanup);

	    dest.removeListener('close', cleanup);
	  }

	  source.on('end', cleanup);
	  source.on('close', cleanup);

	  dest.on('close', cleanup);

	  dest.emit('pipe', source);

	  // Allow for unix-like usage: A.pipe(B).pipe(C)
	  return dest;
	};


/***/ }),
/* 34 */
/***/ (function(module, exports) {

	// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	function EventEmitter() {
	  this._events = this._events || {};
	  this._maxListeners = this._maxListeners || undefined;
	}
	module.exports = EventEmitter;

	// Backwards-compat with node 0.10.x
	EventEmitter.EventEmitter = EventEmitter;

	EventEmitter.prototype._events = undefined;
	EventEmitter.prototype._maxListeners = undefined;

	// By default EventEmitters will print a warning if more than 10 listeners are
	// added to it. This is a useful default which helps finding memory leaks.
	EventEmitter.defaultMaxListeners = 10;

	// Obviously not all Emitters should be limited to 10. This function allows
	// that to be increased. Set to zero for unlimited.
	EventEmitter.prototype.setMaxListeners = function(n) {
	  if (!isNumber(n) || n < 0 || isNaN(n))
	    throw TypeError('n must be a positive number');
	  this._maxListeners = n;
	  return this;
	};

	EventEmitter.prototype.emit = function(type) {
	  var er, handler, len, args, i, listeners;

	  if (!this._events)
	    this._events = {};

	  // If there is no 'error' event listener then throw.
	  if (type === 'error') {
	    if (!this._events.error ||
	        (isObject(this._events.error) && !this._events.error.length)) {
	      er = arguments[1];
	      if (er instanceof Error) {
	        throw er; // Unhandled 'error' event
	      } else {
	        // At least give some kind of context to the user
	        var err = new Error('Uncaught, unspecified "error" event. (' + er + ')');
	        err.context = er;
	        throw err;
	      }
	    }
	  }

	  handler = this._events[type];

	  if (isUndefined(handler))
	    return false;

	  if (isFunction(handler)) {
	    switch (arguments.length) {
	      // fast cases
	      case 1:
	        handler.call(this);
	        break;
	      case 2:
	        handler.call(this, arguments[1]);
	        break;
	      case 3:
	        handler.call(this, arguments[1], arguments[2]);
	        break;
	      // slower
	      default:
	        args = Array.prototype.slice.call(arguments, 1);
	        handler.apply(this, args);
	    }
	  } else if (isObject(handler)) {
	    args = Array.prototype.slice.call(arguments, 1);
	    listeners = handler.slice();
	    len = listeners.length;
	    for (i = 0; i < len; i++)
	      listeners[i].apply(this, args);
	  }

	  return true;
	};

	EventEmitter.prototype.addListener = function(type, listener) {
	  var m;

	  if (!isFunction(listener))
	    throw TypeError('listener must be a function');

	  if (!this._events)
	    this._events = {};

	  // To avoid recursion in the case that type === "newListener"! Before
	  // adding it to the listeners, first emit "newListener".
	  if (this._events.newListener)
	    this.emit('newListener', type,
	              isFunction(listener.listener) ?
	              listener.listener : listener);

	  if (!this._events[type])
	    // Optimize the case of one listener. Don't need the extra array object.
	    this._events[type] = listener;
	  else if (isObject(this._events[type]))
	    // If we've already got an array, just append.
	    this._events[type].push(listener);
	  else
	    // Adding the second element, need to change to array.
	    this._events[type] = [this._events[type], listener];

	  // Check for listener leak
	  if (isObject(this._events[type]) && !this._events[type].warned) {
	    if (!isUndefined(this._maxListeners)) {
	      m = this._maxListeners;
	    } else {
	      m = EventEmitter.defaultMaxListeners;
	    }

	    if (m && m > 0 && this._events[type].length > m) {
	      this._events[type].warned = true;
	      console.error('(node) warning: possible EventEmitter memory ' +
	                    'leak detected. %d listeners added. ' +
	                    'Use emitter.setMaxListeners() to increase limit.',
	                    this._events[type].length);
	      if (typeof console.trace === 'function') {
	        // not supported in IE 10
	        console.trace();
	      }
	    }
	  }

	  return this;
	};

	EventEmitter.prototype.on = EventEmitter.prototype.addListener;

	EventEmitter.prototype.once = function(type, listener) {
	  if (!isFunction(listener))
	    throw TypeError('listener must be a function');

	  var fired = false;

	  function g() {
	    this.removeListener(type, g);

	    if (!fired) {
	      fired = true;
	      listener.apply(this, arguments);
	    }
	  }

	  g.listener = listener;
	  this.on(type, g);

	  return this;
	};

	// emits a 'removeListener' event iff the listener was removed
	EventEmitter.prototype.removeListener = function(type, listener) {
	  var list, position, length, i;

	  if (!isFunction(listener))
	    throw TypeError('listener must be a function');

	  if (!this._events || !this._events[type])
	    return this;

	  list = this._events[type];
	  length = list.length;
	  position = -1;

	  if (list === listener ||
	      (isFunction(list.listener) && list.listener === listener)) {
	    delete this._events[type];
	    if (this._events.removeListener)
	      this.emit('removeListener', type, listener);

	  } else if (isObject(list)) {
	    for (i = length; i-- > 0;) {
	      if (list[i] === listener ||
	          (list[i].listener && list[i].listener === listener)) {
	        position = i;
	        break;
	      }
	    }

	    if (position < 0)
	      return this;

	    if (list.length === 1) {
	      list.length = 0;
	      delete this._events[type];
	    } else {
	      list.splice(position, 1);
	    }

	    if (this._events.removeListener)
	      this.emit('removeListener', type, listener);
	  }

	  return this;
	};

	EventEmitter.prototype.removeAllListeners = function(type) {
	  var key, listeners;

	  if (!this._events)
	    return this;

	  // not listening for removeListener, no need to emit
	  if (!this._events.removeListener) {
	    if (arguments.length === 0)
	      this._events = {};
	    else if (this._events[type])
	      delete this._events[type];
	    return this;
	  }

	  // emit removeListener for all listeners on all events
	  if (arguments.length === 0) {
	    for (key in this._events) {
	      if (key === 'removeListener') continue;
	      this.removeAllListeners(key);
	    }
	    this.removeAllListeners('removeListener');
	    this._events = {};
	    return this;
	  }

	  listeners = this._events[type];

	  if (isFunction(listeners)) {
	    this.removeListener(type, listeners);
	  } else if (listeners) {
	    // LIFO order
	    while (listeners.length)
	      this.removeListener(type, listeners[listeners.length - 1]);
	  }
	  delete this._events[type];

	  return this;
	};

	EventEmitter.prototype.listeners = function(type) {
	  var ret;
	  if (!this._events || !this._events[type])
	    ret = [];
	  else if (isFunction(this._events[type]))
	    ret = [this._events[type]];
	  else
	    ret = this._events[type].slice();
	  return ret;
	};

	EventEmitter.prototype.listenerCount = function(type) {
	  if (this._events) {
	    var evlistener = this._events[type];

	    if (isFunction(evlistener))
	      return 1;
	    else if (evlistener)
	      return evlistener.length;
	  }
	  return 0;
	};

	EventEmitter.listenerCount = function(emitter, type) {
	  return emitter.listenerCount(type);
	};

	function isFunction(arg) {
	  return typeof arg === 'function';
	}

	function isNumber(arg) {
	  return typeof arg === 'number';
	}

	function isObject(arg) {
	  return typeof arg === 'object' && arg !== null;
	}

	function isUndefined(arg) {
	  return arg === void 0;
	}


/***/ }),
/* 35 */
/***/ (function(module, exports) {

	if (typeof Object.create === 'function') {
	  // implementation from standard node.js 'util' module
	  module.exports = function inherits(ctor, superCtor) {
	    ctor.super_ = superCtor
	    ctor.prototype = Object.create(superCtor.prototype, {
	      constructor: {
	        value: ctor,
	        enumerable: false,
	        writable: true,
	        configurable: true
	      }
	    });
	  };
	} else {
	  // old school shim for old browsers
	  module.exports = function inherits(ctor, superCtor) {
	    ctor.super_ = superCtor
	    var TempCtor = function () {}
	    TempCtor.prototype = superCtor.prototype
	    ctor.prototype = new TempCtor()
	    ctor.prototype.constructor = ctor
	  }
	}


/***/ }),
/* 36 */
/***/ (function(module, exports, __webpack_require__) {

	exports = module.exports = __webpack_require__(37);
	exports.Stream = exports;
	exports.Readable = exports;
	exports.Writable = __webpack_require__(48);
	exports.Duplex = __webpack_require__(47);
	exports.Transform = __webpack_require__(53);
	exports.PassThrough = __webpack_require__(54);


/***/ }),
/* 37 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(global, process) {// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	'use strict';

	/*<replacement>*/

	var pna = __webpack_require__(38);
	/*</replacement>*/

	module.exports = Readable;

	/*<replacement>*/
	var isArray = __webpack_require__(39);
	/*</replacement>*/

	/*<replacement>*/
	var Duplex;
	/*</replacement>*/

	Readable.ReadableState = ReadableState;

	/*<replacement>*/
	var EE = __webpack_require__(34).EventEmitter;

	var EElistenerCount = function (emitter, type) {
	  return emitter.listeners(type).length;
	};
	/*</replacement>*/

	/*<replacement>*/
	var Stream = __webpack_require__(40);
	/*</replacement>*/

	/*<replacement>*/

	var Buffer = __webpack_require__(41).Buffer;
	var OurUint8Array = global.Uint8Array || function () {};
	function _uint8ArrayToBuffer(chunk) {
	  return Buffer.from(chunk);
	}
	function _isUint8Array(obj) {
	  return Buffer.isBuffer(obj) || obj instanceof OurUint8Array;
	}

	/*</replacement>*/

	/*<replacement>*/
	var util = __webpack_require__(42);
	util.inherits = __webpack_require__(35);
	/*</replacement>*/

	/*<replacement>*/
	var debugUtil = __webpack_require__(43);
	var debug = void 0;
	if (debugUtil && debugUtil.debuglog) {
	  debug = debugUtil.debuglog('stream');
	} else {
	  debug = function () {};
	}
	/*</replacement>*/

	var BufferList = __webpack_require__(44);
	var destroyImpl = __webpack_require__(46);
	var StringDecoder;

	util.inherits(Readable, Stream);

	var kProxyEvents = ['error', 'close', 'destroy', 'pause', 'resume'];

	function prependListener(emitter, event, fn) {
	  // Sadly this is not cacheable as some libraries bundle their own
	  // event emitter implementation with them.
	  if (typeof emitter.prependListener === 'function') return emitter.prependListener(event, fn);

	  // This is a hack to make sure that our error handler is attached before any
	  // userland ones.  NEVER DO THIS. This is here only because this code needs
	  // to continue to work with older versions of Node.js that do not include
	  // the prependListener() method. The goal is to eventually remove this hack.
	  if (!emitter._events || !emitter._events[event]) emitter.on(event, fn);else if (isArray(emitter._events[event])) emitter._events[event].unshift(fn);else emitter._events[event] = [fn, emitter._events[event]];
	}

	function ReadableState(options, stream) {
	  Duplex = Duplex || __webpack_require__(47);

	  options = options || {};

	  // Duplex streams are both readable and writable, but share
	  // the same options object.
	  // However, some cases require setting options to different
	  // values for the readable and the writable sides of the duplex stream.
	  // These options can be provided separately as readableXXX and writableXXX.
	  var isDuplex = stream instanceof Duplex;

	  // object stream flag. Used to make read(n) ignore n and to
	  // make all the buffer merging and length checks go away
	  this.objectMode = !!options.objectMode;

	  if (isDuplex) this.objectMode = this.objectMode || !!options.readableObjectMode;

	  // the point at which it stops calling _read() to fill the buffer
	  // Note: 0 is a valid value, means "don't call _read preemptively ever"
	  var hwm = options.highWaterMark;
	  var readableHwm = options.readableHighWaterMark;
	  var defaultHwm = this.objectMode ? 16 : 16 * 1024;

	  if (hwm || hwm === 0) this.highWaterMark = hwm;else if (isDuplex && (readableHwm || readableHwm === 0)) this.highWaterMark = readableHwm;else this.highWaterMark = defaultHwm;

	  // cast to ints.
	  this.highWaterMark = Math.floor(this.highWaterMark);

	  // A linked list is used to store data chunks instead of an array because the
	  // linked list can remove elements from the beginning faster than
	  // array.shift()
	  this.buffer = new BufferList();
	  this.length = 0;
	  this.pipes = null;
	  this.pipesCount = 0;
	  this.flowing = null;
	  this.ended = false;
	  this.endEmitted = false;
	  this.reading = false;

	  // a flag to be able to tell if the event 'readable'/'data' is emitted
	  // immediately, or on a later tick.  We set this to true at first, because
	  // any actions that shouldn't happen until "later" should generally also
	  // not happen before the first read call.
	  this.sync = true;

	  // whenever we return null, then we set a flag to say
	  // that we're awaiting a 'readable' event emission.
	  this.needReadable = false;
	  this.emittedReadable = false;
	  this.readableListening = false;
	  this.resumeScheduled = false;

	  // has it been destroyed
	  this.destroyed = false;

	  // Crypto is kind of old and crusty.  Historically, its default string
	  // encoding is 'binary' so we have to make this configurable.
	  // Everything else in the universe uses 'utf8', though.
	  this.defaultEncoding = options.defaultEncoding || 'utf8';

	  // the number of writers that are awaiting a drain event in .pipe()s
	  this.awaitDrain = 0;

	  // if true, a maybeReadMore has been scheduled
	  this.readingMore = false;

	  this.decoder = null;
	  this.encoding = null;
	  if (options.encoding) {
	    if (!StringDecoder) StringDecoder = __webpack_require__(52).StringDecoder;
	    this.decoder = new StringDecoder(options.encoding);
	    this.encoding = options.encoding;
	  }
	}

	function Readable(options) {
	  Duplex = Duplex || __webpack_require__(47);

	  if (!(this instanceof Readable)) return new Readable(options);

	  this._readableState = new ReadableState(options, this);

	  // legacy
	  this.readable = true;

	  if (options) {
	    if (typeof options.read === 'function') this._read = options.read;

	    if (typeof options.destroy === 'function') this._destroy = options.destroy;
	  }

	  Stream.call(this);
	}

	Object.defineProperty(Readable.prototype, 'destroyed', {
	  get: function () {
	    if (this._readableState === undefined) {
	      return false;
	    }
	    return this._readableState.destroyed;
	  },
	  set: function (value) {
	    // we ignore the value if the stream
	    // has not been initialized yet
	    if (!this._readableState) {
	      return;
	    }

	    // backward compatibility, the user is explicitly
	    // managing destroyed
	    this._readableState.destroyed = value;
	  }
	});

	Readable.prototype.destroy = destroyImpl.destroy;
	Readable.prototype._undestroy = destroyImpl.undestroy;
	Readable.prototype._destroy = function (err, cb) {
	  this.push(null);
	  cb(err);
	};

	// Manually shove something into the read() buffer.
	// This returns true if the highWaterMark has not been hit yet,
	// similar to how Writable.write() returns true if you should
	// write() some more.
	Readable.prototype.push = function (chunk, encoding) {
	  var state = this._readableState;
	  var skipChunkCheck;

	  if (!state.objectMode) {
	    if (typeof chunk === 'string') {
	      encoding = encoding || state.defaultEncoding;
	      if (encoding !== state.encoding) {
	        chunk = Buffer.from(chunk, encoding);
	        encoding = '';
	      }
	      skipChunkCheck = true;
	    }
	  } else {
	    skipChunkCheck = true;
	  }

	  return readableAddChunk(this, chunk, encoding, false, skipChunkCheck);
	};

	// Unshift should *always* be something directly out of read()
	Readable.prototype.unshift = function (chunk) {
	  return readableAddChunk(this, chunk, null, true, false);
	};

	function readableAddChunk(stream, chunk, encoding, addToFront, skipChunkCheck) {
	  var state = stream._readableState;
	  if (chunk === null) {
	    state.reading = false;
	    onEofChunk(stream, state);
	  } else {
	    var er;
	    if (!skipChunkCheck) er = chunkInvalid(state, chunk);
	    if (er) {
	      stream.emit('error', er);
	    } else if (state.objectMode || chunk && chunk.length > 0) {
	      if (typeof chunk !== 'string' && !state.objectMode && Object.getPrototypeOf(chunk) !== Buffer.prototype) {
	        chunk = _uint8ArrayToBuffer(chunk);
	      }

	      if (addToFront) {
	        if (state.endEmitted) stream.emit('error', new Error('stream.unshift() after end event'));else addChunk(stream, state, chunk, true);
	      } else if (state.ended) {
	        stream.emit('error', new Error('stream.push() after EOF'));
	      } else {
	        state.reading = false;
	        if (state.decoder && !encoding) {
	          chunk = state.decoder.write(chunk);
	          if (state.objectMode || chunk.length !== 0) addChunk(stream, state, chunk, false);else maybeReadMore(stream, state);
	        } else {
	          addChunk(stream, state, chunk, false);
	        }
	      }
	    } else if (!addToFront) {
	      state.reading = false;
	    }
	  }

	  return needMoreData(state);
	}

	function addChunk(stream, state, chunk, addToFront) {
	  if (state.flowing && state.length === 0 && !state.sync) {
	    stream.emit('data', chunk);
	    stream.read(0);
	  } else {
	    // update the buffer info.
	    state.length += state.objectMode ? 1 : chunk.length;
	    if (addToFront) state.buffer.unshift(chunk);else state.buffer.push(chunk);

	    if (state.needReadable) emitReadable(stream);
	  }
	  maybeReadMore(stream, state);
	}

	function chunkInvalid(state, chunk) {
	  var er;
	  if (!_isUint8Array(chunk) && typeof chunk !== 'string' && chunk !== undefined && !state.objectMode) {
	    er = new TypeError('Invalid non-string/buffer chunk');
	  }
	  return er;
	}

	// if it's past the high water mark, we can push in some more.
	// Also, if we have no data yet, we can stand some
	// more bytes.  This is to work around cases where hwm=0,
	// such as the repl.  Also, if the push() triggered a
	// readable event, and the user called read(largeNumber) such that
	// needReadable was set, then we ought to push more, so that another
	// 'readable' event will be triggered.
	function needMoreData(state) {
	  return !state.ended && (state.needReadable || state.length < state.highWaterMark || state.length === 0);
	}

	Readable.prototype.isPaused = function () {
	  return this._readableState.flowing === false;
	};

	// backwards compatibility.
	Readable.prototype.setEncoding = function (enc) {
	  if (!StringDecoder) StringDecoder = __webpack_require__(52).StringDecoder;
	  this._readableState.decoder = new StringDecoder(enc);
	  this._readableState.encoding = enc;
	  return this;
	};

	// Don't raise the hwm > 8MB
	var MAX_HWM = 0x800000;
	function computeNewHighWaterMark(n) {
	  if (n >= MAX_HWM) {
	    n = MAX_HWM;
	  } else {
	    // Get the next highest power of 2 to prevent increasing hwm excessively in
	    // tiny amounts
	    n--;
	    n |= n >>> 1;
	    n |= n >>> 2;
	    n |= n >>> 4;
	    n |= n >>> 8;
	    n |= n >>> 16;
	    n++;
	  }
	  return n;
	}

	// This function is designed to be inlinable, so please take care when making
	// changes to the function body.
	function howMuchToRead(n, state) {
	  if (n <= 0 || state.length === 0 && state.ended) return 0;
	  if (state.objectMode) return 1;
	  if (n !== n) {
	    // Only flow one buffer at a time
	    if (state.flowing && state.length) return state.buffer.head.data.length;else return state.length;
	  }
	  // If we're asking for more than the current hwm, then raise the hwm.
	  if (n > state.highWaterMark) state.highWaterMark = computeNewHighWaterMark(n);
	  if (n <= state.length) return n;
	  // Don't have enough
	  if (!state.ended) {
	    state.needReadable = true;
	    return 0;
	  }
	  return state.length;
	}

	// you can override either this method, or the async _read(n) below.
	Readable.prototype.read = function (n) {
	  debug('read', n);
	  n = parseInt(n, 10);
	  var state = this._readableState;
	  var nOrig = n;

	  if (n !== 0) state.emittedReadable = false;

	  // if we're doing read(0) to trigger a readable event, but we
	  // already have a bunch of data in the buffer, then just trigger
	  // the 'readable' event and move on.
	  if (n === 0 && state.needReadable && (state.length >= state.highWaterMark || state.ended)) {
	    debug('read: emitReadable', state.length, state.ended);
	    if (state.length === 0 && state.ended) endReadable(this);else emitReadable(this);
	    return null;
	  }

	  n = howMuchToRead(n, state);

	  // if we've ended, and we're now clear, then finish it up.
	  if (n === 0 && state.ended) {
	    if (state.length === 0) endReadable(this);
	    return null;
	  }

	  // All the actual chunk generation logic needs to be
	  // *below* the call to _read.  The reason is that in certain
	  // synthetic stream cases, such as passthrough streams, _read
	  // may be a completely synchronous operation which may change
	  // the state of the read buffer, providing enough data when
	  // before there was *not* enough.
	  //
	  // So, the steps are:
	  // 1. Figure out what the state of things will be after we do
	  // a read from the buffer.
	  //
	  // 2. If that resulting state will trigger a _read, then call _read.
	  // Note that this may be asynchronous, or synchronous.  Yes, it is
	  // deeply ugly to write APIs this way, but that still doesn't mean
	  // that the Readable class should behave improperly, as streams are
	  // designed to be sync/async agnostic.
	  // Take note if the _read call is sync or async (ie, if the read call
	  // has returned yet), so that we know whether or not it's safe to emit
	  // 'readable' etc.
	  //
	  // 3. Actually pull the requested chunks out of the buffer and return.

	  // if we need a readable event, then we need to do some reading.
	  var doRead = state.needReadable;
	  debug('need readable', doRead);

	  // if we currently have less than the highWaterMark, then also read some
	  if (state.length === 0 || state.length - n < state.highWaterMark) {
	    doRead = true;
	    debug('length less than watermark', doRead);
	  }

	  // however, if we've ended, then there's no point, and if we're already
	  // reading, then it's unnecessary.
	  if (state.ended || state.reading) {
	    doRead = false;
	    debug('reading or ended', doRead);
	  } else if (doRead) {
	    debug('do read');
	    state.reading = true;
	    state.sync = true;
	    // if the length is currently zero, then we *need* a readable event.
	    if (state.length === 0) state.needReadable = true;
	    // call internal read method
	    this._read(state.highWaterMark);
	    state.sync = false;
	    // If _read pushed data synchronously, then `reading` will be false,
	    // and we need to re-evaluate how much data we can return to the user.
	    if (!state.reading) n = howMuchToRead(nOrig, state);
	  }

	  var ret;
	  if (n > 0) ret = fromList(n, state);else ret = null;

	  if (ret === null) {
	    state.needReadable = true;
	    n = 0;
	  } else {
	    state.length -= n;
	  }

	  if (state.length === 0) {
	    // If we have nothing in the buffer, then we want to know
	    // as soon as we *do* get something into the buffer.
	    if (!state.ended) state.needReadable = true;

	    // If we tried to read() past the EOF, then emit end on the next tick.
	    if (nOrig !== n && state.ended) endReadable(this);
	  }

	  if (ret !== null) this.emit('data', ret);

	  return ret;
	};

	function onEofChunk(stream, state) {
	  if (state.ended) return;
	  if (state.decoder) {
	    var chunk = state.decoder.end();
	    if (chunk && chunk.length) {
	      state.buffer.push(chunk);
	      state.length += state.objectMode ? 1 : chunk.length;
	    }
	  }
	  state.ended = true;

	  // emit 'readable' now to make sure it gets picked up.
	  emitReadable(stream);
	}

	// Don't emit readable right away in sync mode, because this can trigger
	// another read() call => stack overflow.  This way, it might trigger
	// a nextTick recursion warning, but that's not so bad.
	function emitReadable(stream) {
	  var state = stream._readableState;
	  state.needReadable = false;
	  if (!state.emittedReadable) {
	    debug('emitReadable', state.flowing);
	    state.emittedReadable = true;
	    if (state.sync) pna.nextTick(emitReadable_, stream);else emitReadable_(stream);
	  }
	}

	function emitReadable_(stream) {
	  debug('emit readable');
	  stream.emit('readable');
	  flow(stream);
	}

	// at this point, the user has presumably seen the 'readable' event,
	// and called read() to consume some data.  that may have triggered
	// in turn another _read(n) call, in which case reading = true if
	// it's in progress.
	// However, if we're not ended, or reading, and the length < hwm,
	// then go ahead and try to read some more preemptively.
	function maybeReadMore(stream, state) {
	  if (!state.readingMore) {
	    state.readingMore = true;
	    pna.nextTick(maybeReadMore_, stream, state);
	  }
	}

	function maybeReadMore_(stream, state) {
	  var len = state.length;
	  while (!state.reading && !state.flowing && !state.ended && state.length < state.highWaterMark) {
	    debug('maybeReadMore read 0');
	    stream.read(0);
	    if (len === state.length)
	      // didn't get any data, stop spinning.
	      break;else len = state.length;
	  }
	  state.readingMore = false;
	}

	// abstract method.  to be overridden in specific implementation classes.
	// call cb(er, data) where data is <= n in length.
	// for virtual (non-string, non-buffer) streams, "length" is somewhat
	// arbitrary, and perhaps not very meaningful.
	Readable.prototype._read = function (n) {
	  this.emit('error', new Error('_read() is not implemented'));
	};

	Readable.prototype.pipe = function (dest, pipeOpts) {
	  var src = this;
	  var state = this._readableState;

	  switch (state.pipesCount) {
	    case 0:
	      state.pipes = dest;
	      break;
	    case 1:
	      state.pipes = [state.pipes, dest];
	      break;
	    default:
	      state.pipes.push(dest);
	      break;
	  }
	  state.pipesCount += 1;
	  debug('pipe count=%d opts=%j', state.pipesCount, pipeOpts);

	  var doEnd = (!pipeOpts || pipeOpts.end !== false) && dest !== process.stdout && dest !== process.stderr;

	  var endFn = doEnd ? onend : unpipe;
	  if (state.endEmitted) pna.nextTick(endFn);else src.once('end', endFn);

	  dest.on('unpipe', onunpipe);
	  function onunpipe(readable, unpipeInfo) {
	    debug('onunpipe');
	    if (readable === src) {
	      if (unpipeInfo && unpipeInfo.hasUnpiped === false) {
	        unpipeInfo.hasUnpiped = true;
	        cleanup();
	      }
	    }
	  }

	  function onend() {
	    debug('onend');
	    dest.end();
	  }

	  // when the dest drains, it reduces the awaitDrain counter
	  // on the source.  This would be more elegant with a .once()
	  // handler in flow(), but adding and removing repeatedly is
	  // too slow.
	  var ondrain = pipeOnDrain(src);
	  dest.on('drain', ondrain);

	  var cleanedUp = false;
	  function cleanup() {
	    debug('cleanup');
	    // cleanup event handlers once the pipe is broken
	    dest.removeListener('close', onclose);
	    dest.removeListener('finish', onfinish);
	    dest.removeListener('drain', ondrain);
	    dest.removeListener('error', onerror);
	    dest.removeListener('unpipe', onunpipe);
	    src.removeListener('end', onend);
	    src.removeListener('end', unpipe);
	    src.removeListener('data', ondata);

	    cleanedUp = true;

	    // if the reader is waiting for a drain event from this
	    // specific writer, then it would cause it to never start
	    // flowing again.
	    // So, if this is awaiting a drain, then we just call it now.
	    // If we don't know, then assume that we are waiting for one.
	    if (state.awaitDrain && (!dest._writableState || dest._writableState.needDrain)) ondrain();
	  }

	  // If the user pushes more data while we're writing to dest then we'll end up
	  // in ondata again. However, we only want to increase awaitDrain once because
	  // dest will only emit one 'drain' event for the multiple writes.
	  // => Introduce a guard on increasing awaitDrain.
	  var increasedAwaitDrain = false;
	  src.on('data', ondata);
	  function ondata(chunk) {
	    debug('ondata');
	    increasedAwaitDrain = false;
	    var ret = dest.write(chunk);
	    if (false === ret && !increasedAwaitDrain) {
	      // If the user unpiped during `dest.write()`, it is possible
	      // to get stuck in a permanently paused state if that write
	      // also returned false.
	      // => Check whether `dest` is still a piping destination.
	      if ((state.pipesCount === 1 && state.pipes === dest || state.pipesCount > 1 && indexOf(state.pipes, dest) !== -1) && !cleanedUp) {
	        debug('false write response, pause', src._readableState.awaitDrain);
	        src._readableState.awaitDrain++;
	        increasedAwaitDrain = true;
	      }
	      src.pause();
	    }
	  }

	  // if the dest has an error, then stop piping into it.
	  // however, don't suppress the throwing behavior for this.
	  function onerror(er) {
	    debug('onerror', er);
	    unpipe();
	    dest.removeListener('error', onerror);
	    if (EElistenerCount(dest, 'error') === 0) dest.emit('error', er);
	  }

	  // Make sure our error handler is attached before userland ones.
	  prependListener(dest, 'error', onerror);

	  // Both close and finish should trigger unpipe, but only once.
	  function onclose() {
	    dest.removeListener('finish', onfinish);
	    unpipe();
	  }
	  dest.once('close', onclose);
	  function onfinish() {
	    debug('onfinish');
	    dest.removeListener('close', onclose);
	    unpipe();
	  }
	  dest.once('finish', onfinish);

	  function unpipe() {
	    debug('unpipe');
	    src.unpipe(dest);
	  }

	  // tell the dest that it's being piped to
	  dest.emit('pipe', src);

	  // start the flow if it hasn't been started already.
	  if (!state.flowing) {
	    debug('pipe resume');
	    src.resume();
	  }

	  return dest;
	};

	function pipeOnDrain(src) {
	  return function () {
	    var state = src._readableState;
	    debug('pipeOnDrain', state.awaitDrain);
	    if (state.awaitDrain) state.awaitDrain--;
	    if (state.awaitDrain === 0 && EElistenerCount(src, 'data')) {
	      state.flowing = true;
	      flow(src);
	    }
	  };
	}

	Readable.prototype.unpipe = function (dest) {
	  var state = this._readableState;
	  var unpipeInfo = { hasUnpiped: false };

	  // if we're not piping anywhere, then do nothing.
	  if (state.pipesCount === 0) return this;

	  // just one destination.  most common case.
	  if (state.pipesCount === 1) {
	    // passed in one, but it's not the right one.
	    if (dest && dest !== state.pipes) return this;

	    if (!dest) dest = state.pipes;

	    // got a match.
	    state.pipes = null;
	    state.pipesCount = 0;
	    state.flowing = false;
	    if (dest) dest.emit('unpipe', this, unpipeInfo);
	    return this;
	  }

	  // slow case. multiple pipe destinations.

	  if (!dest) {
	    // remove all.
	    var dests = state.pipes;
	    var len = state.pipesCount;
	    state.pipes = null;
	    state.pipesCount = 0;
	    state.flowing = false;

	    for (var i = 0; i < len; i++) {
	      dests[i].emit('unpipe', this, unpipeInfo);
	    }return this;
	  }

	  // try to find the right one.
	  var index = indexOf(state.pipes, dest);
	  if (index === -1) return this;

	  state.pipes.splice(index, 1);
	  state.pipesCount -= 1;
	  if (state.pipesCount === 1) state.pipes = state.pipes[0];

	  dest.emit('unpipe', this, unpipeInfo);

	  return this;
	};

	// set up data events if they are asked for
	// Ensure readable listeners eventually get something
	Readable.prototype.on = function (ev, fn) {
	  var res = Stream.prototype.on.call(this, ev, fn);

	  if (ev === 'data') {
	    // Start flowing on next tick if stream isn't explicitly paused
	    if (this._readableState.flowing !== false) this.resume();
	  } else if (ev === 'readable') {
	    var state = this._readableState;
	    if (!state.endEmitted && !state.readableListening) {
	      state.readableListening = state.needReadable = true;
	      state.emittedReadable = false;
	      if (!state.reading) {
	        pna.nextTick(nReadingNextTick, this);
	      } else if (state.length) {
	        emitReadable(this);
	      }
	    }
	  }

	  return res;
	};
	Readable.prototype.addListener = Readable.prototype.on;

	function nReadingNextTick(self) {
	  debug('readable nexttick read 0');
	  self.read(0);
	}

	// pause() and resume() are remnants of the legacy readable stream API
	// If the user uses them, then switch into old mode.
	Readable.prototype.resume = function () {
	  var state = this._readableState;
	  if (!state.flowing) {
	    debug('resume');
	    state.flowing = true;
	    resume(this, state);
	  }
	  return this;
	};

	function resume(stream, state) {
	  if (!state.resumeScheduled) {
	    state.resumeScheduled = true;
	    pna.nextTick(resume_, stream, state);
	  }
	}

	function resume_(stream, state) {
	  if (!state.reading) {
	    debug('resume read 0');
	    stream.read(0);
	  }

	  state.resumeScheduled = false;
	  state.awaitDrain = 0;
	  stream.emit('resume');
	  flow(stream);
	  if (state.flowing && !state.reading) stream.read(0);
	}

	Readable.prototype.pause = function () {
	  debug('call pause flowing=%j', this._readableState.flowing);
	  if (false !== this._readableState.flowing) {
	    debug('pause');
	    this._readableState.flowing = false;
	    this.emit('pause');
	  }
	  return this;
	};

	function flow(stream) {
	  var state = stream._readableState;
	  debug('flow', state.flowing);
	  while (state.flowing && stream.read() !== null) {}
	}

	// wrap an old-style stream as the async data source.
	// This is *not* part of the readable stream interface.
	// It is an ugly unfortunate mess of history.
	Readable.prototype.wrap = function (stream) {
	  var _this = this;

	  var state = this._readableState;
	  var paused = false;

	  stream.on('end', function () {
	    debug('wrapped end');
	    if (state.decoder && !state.ended) {
	      var chunk = state.decoder.end();
	      if (chunk && chunk.length) _this.push(chunk);
	    }

	    _this.push(null);
	  });

	  stream.on('data', function (chunk) {
	    debug('wrapped data');
	    if (state.decoder) chunk = state.decoder.write(chunk);

	    // don't skip over falsy values in objectMode
	    if (state.objectMode && (chunk === null || chunk === undefined)) return;else if (!state.objectMode && (!chunk || !chunk.length)) return;

	    var ret = _this.push(chunk);
	    if (!ret) {
	      paused = true;
	      stream.pause();
	    }
	  });

	  // proxy all the other methods.
	  // important when wrapping filters and duplexes.
	  for (var i in stream) {
	    if (this[i] === undefined && typeof stream[i] === 'function') {
	      this[i] = function (method) {
	        return function () {
	          return stream[method].apply(stream, arguments);
	        };
	      }(i);
	    }
	  }

	  // proxy certain important events.
	  for (var n = 0; n < kProxyEvents.length; n++) {
	    stream.on(kProxyEvents[n], this.emit.bind(this, kProxyEvents[n]));
	  }

	  // when we try to consume some more bytes, simply unpause the
	  // underlying stream.
	  this._read = function (n) {
	    debug('wrapped _read', n);
	    if (paused) {
	      paused = false;
	      stream.resume();
	    }
	  };

	  return this;
	};

	// exposed for testing purposes only.
	Readable._fromList = fromList;

	// Pluck off n bytes from an array of buffers.
	// Length is the combined lengths of all the buffers in the list.
	// This function is designed to be inlinable, so please take care when making
	// changes to the function body.
	function fromList(n, state) {
	  // nothing buffered
	  if (state.length === 0) return null;

	  var ret;
	  if (state.objectMode) ret = state.buffer.shift();else if (!n || n >= state.length) {
	    // read it all, truncate the list
	    if (state.decoder) ret = state.buffer.join('');else if (state.buffer.length === 1) ret = state.buffer.head.data;else ret = state.buffer.concat(state.length);
	    state.buffer.clear();
	  } else {
	    // read part of list
	    ret = fromListPartial(n, state.buffer, state.decoder);
	  }

	  return ret;
	}

	// Extracts only enough buffered data to satisfy the amount requested.
	// This function is designed to be inlinable, so please take care when making
	// changes to the function body.
	function fromListPartial(n, list, hasStrings) {
	  var ret;
	  if (n < list.head.data.length) {
	    // slice is the same for buffers and strings
	    ret = list.head.data.slice(0, n);
	    list.head.data = list.head.data.slice(n);
	  } else if (n === list.head.data.length) {
	    // first chunk is a perfect match
	    ret = list.shift();
	  } else {
	    // result spans more than one buffer
	    ret = hasStrings ? copyFromBufferString(n, list) : copyFromBuffer(n, list);
	  }
	  return ret;
	}

	// Copies a specified amount of characters from the list of buffered data
	// chunks.
	// This function is designed to be inlinable, so please take care when making
	// changes to the function body.
	function copyFromBufferString(n, list) {
	  var p = list.head;
	  var c = 1;
	  var ret = p.data;
	  n -= ret.length;
	  while (p = p.next) {
	    var str = p.data;
	    var nb = n > str.length ? str.length : n;
	    if (nb === str.length) ret += str;else ret += str.slice(0, n);
	    n -= nb;
	    if (n === 0) {
	      if (nb === str.length) {
	        ++c;
	        if (p.next) list.head = p.next;else list.head = list.tail = null;
	      } else {
	        list.head = p;
	        p.data = str.slice(nb);
	      }
	      break;
	    }
	    ++c;
	  }
	  list.length -= c;
	  return ret;
	}

	// Copies a specified amount of bytes from the list of buffered data chunks.
	// This function is designed to be inlinable, so please take care when making
	// changes to the function body.
	function copyFromBuffer(n, list) {
	  var ret = Buffer.allocUnsafe(n);
	  var p = list.head;
	  var c = 1;
	  p.data.copy(ret);
	  n -= p.data.length;
	  while (p = p.next) {
	    var buf = p.data;
	    var nb = n > buf.length ? buf.length : n;
	    buf.copy(ret, ret.length - n, 0, nb);
	    n -= nb;
	    if (n === 0) {
	      if (nb === buf.length) {
	        ++c;
	        if (p.next) list.head = p.next;else list.head = list.tail = null;
	      } else {
	        list.head = p;
	        p.data = buf.slice(nb);
	      }
	      break;
	    }
	    ++c;
	  }
	  list.length -= c;
	  return ret;
	}

	function endReadable(stream) {
	  var state = stream._readableState;

	  // If we get here before consuming all the bytes, then that is a
	  // bug in node.  Should never happen.
	  if (state.length > 0) throw new Error('"endReadable()" called on non-empty stream');

	  if (!state.endEmitted) {
	    state.ended = true;
	    pna.nextTick(endReadableNT, state, stream);
	  }
	}

	function endReadableNT(state, stream) {
	  // Check that we didn't get one last unshift.
	  if (!state.endEmitted && state.length === 0) {
	    state.endEmitted = true;
	    stream.readable = false;
	    stream.emit('end');
	  }
	}

	function forEach(xs, f) {
	  for (var i = 0, l = xs.length; i < l; i++) {
	    f(xs[i], i);
	  }
	}

	function indexOf(xs, x) {
	  for (var i = 0, l = xs.length; i < l; i++) {
	    if (xs[i] === x) return i;
	  }
	  return -1;
	}
	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }()), __webpack_require__(18)))

/***/ }),
/* 38 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(process) {'use strict';

	if (!process.version ||
	    process.version.indexOf('v0.') === 0 ||
	    process.version.indexOf('v1.') === 0 && process.version.indexOf('v1.8.') !== 0) {
	  module.exports = { nextTick: nextTick };
	} else {
	  module.exports = process
	}

	function nextTick(fn, arg1, arg2, arg3) {
	  if (typeof fn !== 'function') {
	    throw new TypeError('"callback" argument must be a function');
	  }
	  var len = arguments.length;
	  var args, i;
	  switch (len) {
	  case 0:
	  case 1:
	    return process.nextTick(fn);
	  case 2:
	    return process.nextTick(function afterTickOne() {
	      fn.call(null, arg1);
	    });
	  case 3:
	    return process.nextTick(function afterTickTwo() {
	      fn.call(null, arg1, arg2);
	    });
	  case 4:
	    return process.nextTick(function afterTickThree() {
	      fn.call(null, arg1, arg2, arg3);
	    });
	  default:
	    args = new Array(len - 1);
	    i = 0;
	    while (i < args.length) {
	      args[i++] = arguments[i];
	    }
	    return process.nextTick(function afterTick() {
	      fn.apply(null, args);
	    });
	  }
	}


	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(18)))

/***/ }),
/* 39 */
/***/ (function(module, exports) {

	var toString = {}.toString;

	module.exports = Array.isArray || function (arr) {
	  return toString.call(arr) == '[object Array]';
	};


/***/ }),
/* 40 */
/***/ (function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__(34).EventEmitter;


/***/ }),
/* 41 */
/***/ (function(module, exports, __webpack_require__) {

	/* eslint-disable node/no-deprecated-api */
	var buffer = __webpack_require__(5)
	var Buffer = buffer.Buffer

	// alternative to using Object.keys for old browsers
	function copyProps (src, dst) {
	  for (var key in src) {
	    dst[key] = src[key]
	  }
	}
	if (Buffer.from && Buffer.alloc && Buffer.allocUnsafe && Buffer.allocUnsafeSlow) {
	  module.exports = buffer
	} else {
	  // Copy properties from require('buffer')
	  copyProps(buffer, exports)
	  exports.Buffer = SafeBuffer
	}

	function SafeBuffer (arg, encodingOrOffset, length) {
	  return Buffer(arg, encodingOrOffset, length)
	}

	// Copy static methods from Buffer
	copyProps(Buffer, SafeBuffer)

	SafeBuffer.from = function (arg, encodingOrOffset, length) {
	  if (typeof arg === 'number') {
	    throw new TypeError('Argument must not be a number')
	  }
	  return Buffer(arg, encodingOrOffset, length)
	}

	SafeBuffer.alloc = function (size, fill, encoding) {
	  if (typeof size !== 'number') {
	    throw new TypeError('Argument must be a number')
	  }
	  var buf = Buffer(size)
	  if (fill !== undefined) {
	    if (typeof encoding === 'string') {
	      buf.fill(fill, encoding)
	    } else {
	      buf.fill(fill)
	    }
	  } else {
	    buf.fill(0)
	  }
	  return buf
	}

	SafeBuffer.allocUnsafe = function (size) {
	  if (typeof size !== 'number') {
	    throw new TypeError('Argument must be a number')
	  }
	  return Buffer(size)
	}

	SafeBuffer.allocUnsafeSlow = function (size) {
	  if (typeof size !== 'number') {
	    throw new TypeError('Argument must be a number')
	  }
	  return buffer.SlowBuffer(size)
	}


/***/ }),
/* 42 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	// NOTE: These type checking functions intentionally don't use `instanceof`
	// because it is fragile and can be easily faked with `Object.create()`.

	function isArray(arg) {
	  if (Array.isArray) {
	    return Array.isArray(arg);
	  }
	  return objectToString(arg) === '[object Array]';
	}
	exports.isArray = isArray;

	function isBoolean(arg) {
	  return typeof arg === 'boolean';
	}
	exports.isBoolean = isBoolean;

	function isNull(arg) {
	  return arg === null;
	}
	exports.isNull = isNull;

	function isNullOrUndefined(arg) {
	  return arg == null;
	}
	exports.isNullOrUndefined = isNullOrUndefined;

	function isNumber(arg) {
	  return typeof arg === 'number';
	}
	exports.isNumber = isNumber;

	function isString(arg) {
	  return typeof arg === 'string';
	}
	exports.isString = isString;

	function isSymbol(arg) {
	  return typeof arg === 'symbol';
	}
	exports.isSymbol = isSymbol;

	function isUndefined(arg) {
	  return arg === void 0;
	}
	exports.isUndefined = isUndefined;

	function isRegExp(re) {
	  return objectToString(re) === '[object RegExp]';
	}
	exports.isRegExp = isRegExp;

	function isObject(arg) {
	  return typeof arg === 'object' && arg !== null;
	}
	exports.isObject = isObject;

	function isDate(d) {
	  return objectToString(d) === '[object Date]';
	}
	exports.isDate = isDate;

	function isError(e) {
	  return (objectToString(e) === '[object Error]' || e instanceof Error);
	}
	exports.isError = isError;

	function isFunction(arg) {
	  return typeof arg === 'function';
	}
	exports.isFunction = isFunction;

	function isPrimitive(arg) {
	  return arg === null ||
	         typeof arg === 'boolean' ||
	         typeof arg === 'number' ||
	         typeof arg === 'string' ||
	         typeof arg === 'symbol' ||  // ES6 symbol
	         typeof arg === 'undefined';
	}
	exports.isPrimitive = isPrimitive;

	exports.isBuffer = Buffer.isBuffer;

	function objectToString(o) {
	  return Object.prototype.toString.call(o);
	}

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 43 */
/***/ (function(module, exports) {

	/* (ignored) */

/***/ }),
/* 44 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

	var Buffer = __webpack_require__(41).Buffer;
	var util = __webpack_require__(45);

	function copyBuffer(src, target, offset) {
	  src.copy(target, offset);
	}

	module.exports = function () {
	  function BufferList() {
	    _classCallCheck(this, BufferList);

	    this.head = null;
	    this.tail = null;
	    this.length = 0;
	  }

	  BufferList.prototype.push = function push(v) {
	    var entry = { data: v, next: null };
	    if (this.length > 0) this.tail.next = entry;else this.head = entry;
	    this.tail = entry;
	    ++this.length;
	  };

	  BufferList.prototype.unshift = function unshift(v) {
	    var entry = { data: v, next: this.head };
	    if (this.length === 0) this.tail = entry;
	    this.head = entry;
	    ++this.length;
	  };

	  BufferList.prototype.shift = function shift() {
	    if (this.length === 0) return;
	    var ret = this.head.data;
	    if (this.length === 1) this.head = this.tail = null;else this.head = this.head.next;
	    --this.length;
	    return ret;
	  };

	  BufferList.prototype.clear = function clear() {
	    this.head = this.tail = null;
	    this.length = 0;
	  };

	  BufferList.prototype.join = function join(s) {
	    if (this.length === 0) return '';
	    var p = this.head;
	    var ret = '' + p.data;
	    while (p = p.next) {
	      ret += s + p.data;
	    }return ret;
	  };

	  BufferList.prototype.concat = function concat(n) {
	    if (this.length === 0) return Buffer.alloc(0);
	    if (this.length === 1) return this.head.data;
	    var ret = Buffer.allocUnsafe(n >>> 0);
	    var p = this.head;
	    var i = 0;
	    while (p) {
	      copyBuffer(p.data, ret, i);
	      i += p.data.length;
	      p = p.next;
	    }
	    return ret;
	  };

	  return BufferList;
	}();

	if (util && util.inspect && util.inspect.custom) {
	  module.exports.prototype[util.inspect.custom] = function () {
	    var obj = util.inspect({ length: this.length });
	    return this.constructor.name + ' ' + obj;
	  };
	}

/***/ }),
/* 45 */
/***/ (function(module, exports) {

	/* (ignored) */

/***/ }),
/* 46 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	/*<replacement>*/

	var pna = __webpack_require__(38);
	/*</replacement>*/

	// undocumented cb() API, needed for core, not for public API
	function destroy(err, cb) {
	  var _this = this;

	  var readableDestroyed = this._readableState && this._readableState.destroyed;
	  var writableDestroyed = this._writableState && this._writableState.destroyed;

	  if (readableDestroyed || writableDestroyed) {
	    if (cb) {
	      cb(err);
	    } else if (err && (!this._writableState || !this._writableState.errorEmitted)) {
	      pna.nextTick(emitErrorNT, this, err);
	    }
	    return this;
	  }

	  // we set destroyed to true before firing error callbacks in order
	  // to make it re-entrance safe in case destroy() is called within callbacks

	  if (this._readableState) {
	    this._readableState.destroyed = true;
	  }

	  // if this is a duplex stream mark the writable part as destroyed as well
	  if (this._writableState) {
	    this._writableState.destroyed = true;
	  }

	  this._destroy(err || null, function (err) {
	    if (!cb && err) {
	      pna.nextTick(emitErrorNT, _this, err);
	      if (_this._writableState) {
	        _this._writableState.errorEmitted = true;
	      }
	    } else if (cb) {
	      cb(err);
	    }
	  });

	  return this;
	}

	function undestroy() {
	  if (this._readableState) {
	    this._readableState.destroyed = false;
	    this._readableState.reading = false;
	    this._readableState.ended = false;
	    this._readableState.endEmitted = false;
	  }

	  if (this._writableState) {
	    this._writableState.destroyed = false;
	    this._writableState.ended = false;
	    this._writableState.ending = false;
	    this._writableState.finished = false;
	    this._writableState.errorEmitted = false;
	  }
	}

	function emitErrorNT(self, err) {
	  self.emit('error', err);
	}

	module.exports = {
	  destroy: destroy,
	  undestroy: undestroy
	};

/***/ }),
/* 47 */
/***/ (function(module, exports, __webpack_require__) {

	// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	// a duplex stream is just a stream that is both readable and writable.
	// Since JS doesn't have multiple prototypal inheritance, this class
	// prototypally inherits from Readable, and then parasitically from
	// Writable.

	'use strict';

	/*<replacement>*/

	var pna = __webpack_require__(38);
	/*</replacement>*/

	/*<replacement>*/
	var objectKeys = Object.keys || function (obj) {
	  var keys = [];
	  for (var key in obj) {
	    keys.push(key);
	  }return keys;
	};
	/*</replacement>*/

	module.exports = Duplex;

	/*<replacement>*/
	var util = __webpack_require__(42);
	util.inherits = __webpack_require__(35);
	/*</replacement>*/

	var Readable = __webpack_require__(37);
	var Writable = __webpack_require__(48);

	util.inherits(Duplex, Readable);

	var keys = objectKeys(Writable.prototype);
	for (var v = 0; v < keys.length; v++) {
	  var method = keys[v];
	  if (!Duplex.prototype[method]) Duplex.prototype[method] = Writable.prototype[method];
	}

	function Duplex(options) {
	  if (!(this instanceof Duplex)) return new Duplex(options);

	  Readable.call(this, options);
	  Writable.call(this, options);

	  if (options && options.readable === false) this.readable = false;

	  if (options && options.writable === false) this.writable = false;

	  this.allowHalfOpen = true;
	  if (options && options.allowHalfOpen === false) this.allowHalfOpen = false;

	  this.once('end', onend);
	}

	// the no-half-open enforcer
	function onend() {
	  // if we allow half-open state, or if the writable side ended,
	  // then we're ok.
	  if (this.allowHalfOpen || this._writableState.ended) return;

	  // no more data can be written.
	  // But allow more writes to happen in this tick.
	  pna.nextTick(onEndNT, this);
	}

	function onEndNT(self) {
	  self.end();
	}

	Object.defineProperty(Duplex.prototype, 'destroyed', {
	  get: function () {
	    if (this._readableState === undefined || this._writableState === undefined) {
	      return false;
	    }
	    return this._readableState.destroyed && this._writableState.destroyed;
	  },
	  set: function (value) {
	    // we ignore the value if the stream
	    // has not been initialized yet
	    if (this._readableState === undefined || this._writableState === undefined) {
	      return;
	    }

	    // backward compatibility, the user is explicitly
	    // managing destroyed
	    this._readableState.destroyed = value;
	    this._writableState.destroyed = value;
	  }
	});

	Duplex.prototype._destroy = function (err, cb) {
	  this.push(null);
	  this.end();

	  pna.nextTick(cb, err);
	};

	function forEach(xs, f) {
	  for (var i = 0, l = xs.length; i < l; i++) {
	    f(xs[i], i);
	  }
	}

/***/ }),
/* 48 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(process, setImmediate, global) {// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	// A bit simpler than readable streams.
	// Implement an async ._write(chunk, encoding, cb), and it'll handle all
	// the drain event emission and buffering.

	'use strict';

	/*<replacement>*/

	var pna = __webpack_require__(38);
	/*</replacement>*/

	module.exports = Writable;

	/* <replacement> */
	function WriteReq(chunk, encoding, cb) {
	  this.chunk = chunk;
	  this.encoding = encoding;
	  this.callback = cb;
	  this.next = null;
	}

	// It seems a linked list but it is not
	// there will be only 2 of these for each stream
	function CorkedRequest(state) {
	  var _this = this;

	  this.next = null;
	  this.entry = null;
	  this.finish = function () {
	    onCorkedFinish(_this, state);
	  };
	}
	/* </replacement> */

	/*<replacement>*/
	var asyncWrite = !process.browser && ['v0.10', 'v0.9.'].indexOf(process.version.slice(0, 5)) > -1 ? setImmediate : pna.nextTick;
	/*</replacement>*/

	/*<replacement>*/
	var Duplex;
	/*</replacement>*/

	Writable.WritableState = WritableState;

	/*<replacement>*/
	var util = __webpack_require__(42);
	util.inherits = __webpack_require__(35);
	/*</replacement>*/

	/*<replacement>*/
	var internalUtil = {
	  deprecate: __webpack_require__(51)
	};
	/*</replacement>*/

	/*<replacement>*/
	var Stream = __webpack_require__(40);
	/*</replacement>*/

	/*<replacement>*/

	var Buffer = __webpack_require__(41).Buffer;
	var OurUint8Array = global.Uint8Array || function () {};
	function _uint8ArrayToBuffer(chunk) {
	  return Buffer.from(chunk);
	}
	function _isUint8Array(obj) {
	  return Buffer.isBuffer(obj) || obj instanceof OurUint8Array;
	}

	/*</replacement>*/

	var destroyImpl = __webpack_require__(46);

	util.inherits(Writable, Stream);

	function nop() {}

	function WritableState(options, stream) {
	  Duplex = Duplex || __webpack_require__(47);

	  options = options || {};

	  // Duplex streams are both readable and writable, but share
	  // the same options object.
	  // However, some cases require setting options to different
	  // values for the readable and the writable sides of the duplex stream.
	  // These options can be provided separately as readableXXX and writableXXX.
	  var isDuplex = stream instanceof Duplex;

	  // object stream flag to indicate whether or not this stream
	  // contains buffers or objects.
	  this.objectMode = !!options.objectMode;

	  if (isDuplex) this.objectMode = this.objectMode || !!options.writableObjectMode;

	  // the point at which write() starts returning false
	  // Note: 0 is a valid value, means that we always return false if
	  // the entire buffer is not flushed immediately on write()
	  var hwm = options.highWaterMark;
	  var writableHwm = options.writableHighWaterMark;
	  var defaultHwm = this.objectMode ? 16 : 16 * 1024;

	  if (hwm || hwm === 0) this.highWaterMark = hwm;else if (isDuplex && (writableHwm || writableHwm === 0)) this.highWaterMark = writableHwm;else this.highWaterMark = defaultHwm;

	  // cast to ints.
	  this.highWaterMark = Math.floor(this.highWaterMark);

	  // if _final has been called
	  this.finalCalled = false;

	  // drain event flag.
	  this.needDrain = false;
	  // at the start of calling end()
	  this.ending = false;
	  // when end() has been called, and returned
	  this.ended = false;
	  // when 'finish' is emitted
	  this.finished = false;

	  // has it been destroyed
	  this.destroyed = false;

	  // should we decode strings into buffers before passing to _write?
	  // this is here so that some node-core streams can optimize string
	  // handling at a lower level.
	  var noDecode = options.decodeStrings === false;
	  this.decodeStrings = !noDecode;

	  // Crypto is kind of old and crusty.  Historically, its default string
	  // encoding is 'binary' so we have to make this configurable.
	  // Everything else in the universe uses 'utf8', though.
	  this.defaultEncoding = options.defaultEncoding || 'utf8';

	  // not an actual buffer we keep track of, but a measurement
	  // of how much we're waiting to get pushed to some underlying
	  // socket or file.
	  this.length = 0;

	  // a flag to see when we're in the middle of a write.
	  this.writing = false;

	  // when true all writes will be buffered until .uncork() call
	  this.corked = 0;

	  // a flag to be able to tell if the onwrite cb is called immediately,
	  // or on a later tick.  We set this to true at first, because any
	  // actions that shouldn't happen until "later" should generally also
	  // not happen before the first write call.
	  this.sync = true;

	  // a flag to know if we're processing previously buffered items, which
	  // may call the _write() callback in the same tick, so that we don't
	  // end up in an overlapped onwrite situation.
	  this.bufferProcessing = false;

	  // the callback that's passed to _write(chunk,cb)
	  this.onwrite = function (er) {
	    onwrite(stream, er);
	  };

	  // the callback that the user supplies to write(chunk,encoding,cb)
	  this.writecb = null;

	  // the amount that is being written when _write is called.
	  this.writelen = 0;

	  this.bufferedRequest = null;
	  this.lastBufferedRequest = null;

	  // number of pending user-supplied write callbacks
	  // this must be 0 before 'finish' can be emitted
	  this.pendingcb = 0;

	  // emit prefinish if the only thing we're waiting for is _write cbs
	  // This is relevant for synchronous Transform streams
	  this.prefinished = false;

	  // True if the error was already emitted and should not be thrown again
	  this.errorEmitted = false;

	  // count buffered requests
	  this.bufferedRequestCount = 0;

	  // allocate the first CorkedRequest, there is always
	  // one allocated and free to use, and we maintain at most two
	  this.corkedRequestsFree = new CorkedRequest(this);
	}

	WritableState.prototype.getBuffer = function getBuffer() {
	  var current = this.bufferedRequest;
	  var out = [];
	  while (current) {
	    out.push(current);
	    current = current.next;
	  }
	  return out;
	};

	(function () {
	  try {
	    Object.defineProperty(WritableState.prototype, 'buffer', {
	      get: internalUtil.deprecate(function () {
	        return this.getBuffer();
	      }, '_writableState.buffer is deprecated. Use _writableState.getBuffer ' + 'instead.', 'DEP0003')
	    });
	  } catch (_) {}
	})();

	// Test _writableState for inheritance to account for Duplex streams,
	// whose prototype chain only points to Readable.
	var realHasInstance;
	if (typeof Symbol === 'function' && Symbol.hasInstance && typeof Function.prototype[Symbol.hasInstance] === 'function') {
	  realHasInstance = Function.prototype[Symbol.hasInstance];
	  Object.defineProperty(Writable, Symbol.hasInstance, {
	    value: function (object) {
	      if (realHasInstance.call(this, object)) return true;
	      if (this !== Writable) return false;

	      return object && object._writableState instanceof WritableState;
	    }
	  });
	} else {
	  realHasInstance = function (object) {
	    return object instanceof this;
	  };
	}

	function Writable(options) {
	  Duplex = Duplex || __webpack_require__(47);

	  // Writable ctor is applied to Duplexes, too.
	  // `realHasInstance` is necessary because using plain `instanceof`
	  // would return false, as no `_writableState` property is attached.

	  // Trying to use the custom `instanceof` for Writable here will also break the
	  // Node.js LazyTransform implementation, which has a non-trivial getter for
	  // `_writableState` that would lead to infinite recursion.
	  if (!realHasInstance.call(Writable, this) && !(this instanceof Duplex)) {
	    return new Writable(options);
	  }

	  this._writableState = new WritableState(options, this);

	  // legacy.
	  this.writable = true;

	  if (options) {
	    if (typeof options.write === 'function') this._write = options.write;

	    if (typeof options.writev === 'function') this._writev = options.writev;

	    if (typeof options.destroy === 'function') this._destroy = options.destroy;

	    if (typeof options.final === 'function') this._final = options.final;
	  }

	  Stream.call(this);
	}

	// Otherwise people can pipe Writable streams, which is just wrong.
	Writable.prototype.pipe = function () {
	  this.emit('error', new Error('Cannot pipe, not readable'));
	};

	function writeAfterEnd(stream, cb) {
	  var er = new Error('write after end');
	  // TODO: defer error events consistently everywhere, not just the cb
	  stream.emit('error', er);
	  pna.nextTick(cb, er);
	}

	// Checks that a user-supplied chunk is valid, especially for the particular
	// mode the stream is in. Currently this means that `null` is never accepted
	// and undefined/non-string values are only allowed in object mode.
	function validChunk(stream, state, chunk, cb) {
	  var valid = true;
	  var er = false;

	  if (chunk === null) {
	    er = new TypeError('May not write null values to stream');
	  } else if (typeof chunk !== 'string' && chunk !== undefined && !state.objectMode) {
	    er = new TypeError('Invalid non-string/buffer chunk');
	  }
	  if (er) {
	    stream.emit('error', er);
	    pna.nextTick(cb, er);
	    valid = false;
	  }
	  return valid;
	}

	Writable.prototype.write = function (chunk, encoding, cb) {
	  var state = this._writableState;
	  var ret = false;
	  var isBuf = !state.objectMode && _isUint8Array(chunk);

	  if (isBuf && !Buffer.isBuffer(chunk)) {
	    chunk = _uint8ArrayToBuffer(chunk);
	  }

	  if (typeof encoding === 'function') {
	    cb = encoding;
	    encoding = null;
	  }

	  if (isBuf) encoding = 'buffer';else if (!encoding) encoding = state.defaultEncoding;

	  if (typeof cb !== 'function') cb = nop;

	  if (state.ended) writeAfterEnd(this, cb);else if (isBuf || validChunk(this, state, chunk, cb)) {
	    state.pendingcb++;
	    ret = writeOrBuffer(this, state, isBuf, chunk, encoding, cb);
	  }

	  return ret;
	};

	Writable.prototype.cork = function () {
	  var state = this._writableState;

	  state.corked++;
	};

	Writable.prototype.uncork = function () {
	  var state = this._writableState;

	  if (state.corked) {
	    state.corked--;

	    if (!state.writing && !state.corked && !state.finished && !state.bufferProcessing && state.bufferedRequest) clearBuffer(this, state);
	  }
	};

	Writable.prototype.setDefaultEncoding = function setDefaultEncoding(encoding) {
	  // node::ParseEncoding() requires lower case.
	  if (typeof encoding === 'string') encoding = encoding.toLowerCase();
	  if (!(['hex', 'utf8', 'utf-8', 'ascii', 'binary', 'base64', 'ucs2', 'ucs-2', 'utf16le', 'utf-16le', 'raw'].indexOf((encoding + '').toLowerCase()) > -1)) throw new TypeError('Unknown encoding: ' + encoding);
	  this._writableState.defaultEncoding = encoding;
	  return this;
	};

	function decodeChunk(state, chunk, encoding) {
	  if (!state.objectMode && state.decodeStrings !== false && typeof chunk === 'string') {
	    chunk = Buffer.from(chunk, encoding);
	  }
	  return chunk;
	}

	// if we're already writing something, then just put this
	// in the queue, and wait our turn.  Otherwise, call _write
	// If we return false, then we need a drain event, so set that flag.
	function writeOrBuffer(stream, state, isBuf, chunk, encoding, cb) {
	  if (!isBuf) {
	    var newChunk = decodeChunk(state, chunk, encoding);
	    if (chunk !== newChunk) {
	      isBuf = true;
	      encoding = 'buffer';
	      chunk = newChunk;
	    }
	  }
	  var len = state.objectMode ? 1 : chunk.length;

	  state.length += len;

	  var ret = state.length < state.highWaterMark;
	  // we must ensure that previous needDrain will not be reset to false.
	  if (!ret) state.needDrain = true;

	  if (state.writing || state.corked) {
	    var last = state.lastBufferedRequest;
	    state.lastBufferedRequest = {
	      chunk: chunk,
	      encoding: encoding,
	      isBuf: isBuf,
	      callback: cb,
	      next: null
	    };
	    if (last) {
	      last.next = state.lastBufferedRequest;
	    } else {
	      state.bufferedRequest = state.lastBufferedRequest;
	    }
	    state.bufferedRequestCount += 1;
	  } else {
	    doWrite(stream, state, false, len, chunk, encoding, cb);
	  }

	  return ret;
	}

	function doWrite(stream, state, writev, len, chunk, encoding, cb) {
	  state.writelen = len;
	  state.writecb = cb;
	  state.writing = true;
	  state.sync = true;
	  if (writev) stream._writev(chunk, state.onwrite);else stream._write(chunk, encoding, state.onwrite);
	  state.sync = false;
	}

	function onwriteError(stream, state, sync, er, cb) {
	  --state.pendingcb;

	  if (sync) {
	    // defer the callback if we are being called synchronously
	    // to avoid piling up things on the stack
	    pna.nextTick(cb, er);
	    // this can emit finish, and it will always happen
	    // after error
	    pna.nextTick(finishMaybe, stream, state);
	    stream._writableState.errorEmitted = true;
	    stream.emit('error', er);
	  } else {
	    // the caller expect this to happen before if
	    // it is async
	    cb(er);
	    stream._writableState.errorEmitted = true;
	    stream.emit('error', er);
	    // this can emit finish, but finish must
	    // always follow error
	    finishMaybe(stream, state);
	  }
	}

	function onwriteStateUpdate(state) {
	  state.writing = false;
	  state.writecb = null;
	  state.length -= state.writelen;
	  state.writelen = 0;
	}

	function onwrite(stream, er) {
	  var state = stream._writableState;
	  var sync = state.sync;
	  var cb = state.writecb;

	  onwriteStateUpdate(state);

	  if (er) onwriteError(stream, state, sync, er, cb);else {
	    // Check if we're actually ready to finish, but don't emit yet
	    var finished = needFinish(state);

	    if (!finished && !state.corked && !state.bufferProcessing && state.bufferedRequest) {
	      clearBuffer(stream, state);
	    }

	    if (sync) {
	      /*<replacement>*/
	      asyncWrite(afterWrite, stream, state, finished, cb);
	      /*</replacement>*/
	    } else {
	      afterWrite(stream, state, finished, cb);
	    }
	  }
	}

	function afterWrite(stream, state, finished, cb) {
	  if (!finished) onwriteDrain(stream, state);
	  state.pendingcb--;
	  cb();
	  finishMaybe(stream, state);
	}

	// Must force callback to be called on nextTick, so that we don't
	// emit 'drain' before the write() consumer gets the 'false' return
	// value, and has a chance to attach a 'drain' listener.
	function onwriteDrain(stream, state) {
	  if (state.length === 0 && state.needDrain) {
	    state.needDrain = false;
	    stream.emit('drain');
	  }
	}

	// if there's something in the buffer waiting, then process it
	function clearBuffer(stream, state) {
	  state.bufferProcessing = true;
	  var entry = state.bufferedRequest;

	  if (stream._writev && entry && entry.next) {
	    // Fast case, write everything using _writev()
	    var l = state.bufferedRequestCount;
	    var buffer = new Array(l);
	    var holder = state.corkedRequestsFree;
	    holder.entry = entry;

	    var count = 0;
	    var allBuffers = true;
	    while (entry) {
	      buffer[count] = entry;
	      if (!entry.isBuf) allBuffers = false;
	      entry = entry.next;
	      count += 1;
	    }
	    buffer.allBuffers = allBuffers;

	    doWrite(stream, state, true, state.length, buffer, '', holder.finish);

	    // doWrite is almost always async, defer these to save a bit of time
	    // as the hot path ends with doWrite
	    state.pendingcb++;
	    state.lastBufferedRequest = null;
	    if (holder.next) {
	      state.corkedRequestsFree = holder.next;
	      holder.next = null;
	    } else {
	      state.corkedRequestsFree = new CorkedRequest(state);
	    }
	    state.bufferedRequestCount = 0;
	  } else {
	    // Slow case, write chunks one-by-one
	    while (entry) {
	      var chunk = entry.chunk;
	      var encoding = entry.encoding;
	      var cb = entry.callback;
	      var len = state.objectMode ? 1 : chunk.length;

	      doWrite(stream, state, false, len, chunk, encoding, cb);
	      entry = entry.next;
	      state.bufferedRequestCount--;
	      // if we didn't call the onwrite immediately, then
	      // it means that we need to wait until it does.
	      // also, that means that the chunk and cb are currently
	      // being processed, so move the buffer counter past them.
	      if (state.writing) {
	        break;
	      }
	    }

	    if (entry === null) state.lastBufferedRequest = null;
	  }

	  state.bufferedRequest = entry;
	  state.bufferProcessing = false;
	}

	Writable.prototype._write = function (chunk, encoding, cb) {
	  cb(new Error('_write() is not implemented'));
	};

	Writable.prototype._writev = null;

	Writable.prototype.end = function (chunk, encoding, cb) {
	  var state = this._writableState;

	  if (typeof chunk === 'function') {
	    cb = chunk;
	    chunk = null;
	    encoding = null;
	  } else if (typeof encoding === 'function') {
	    cb = encoding;
	    encoding = null;
	  }

	  if (chunk !== null && chunk !== undefined) this.write(chunk, encoding);

	  // .end() fully uncorks
	  if (state.corked) {
	    state.corked = 1;
	    this.uncork();
	  }

	  // ignore unnecessary end() calls.
	  if (!state.ending && !state.finished) endWritable(this, state, cb);
	};

	function needFinish(state) {
	  return state.ending && state.length === 0 && state.bufferedRequest === null && !state.finished && !state.writing;
	}
	function callFinal(stream, state) {
	  stream._final(function (err) {
	    state.pendingcb--;
	    if (err) {
	      stream.emit('error', err);
	    }
	    state.prefinished = true;
	    stream.emit('prefinish');
	    finishMaybe(stream, state);
	  });
	}
	function prefinish(stream, state) {
	  if (!state.prefinished && !state.finalCalled) {
	    if (typeof stream._final === 'function') {
	      state.pendingcb++;
	      state.finalCalled = true;
	      pna.nextTick(callFinal, stream, state);
	    } else {
	      state.prefinished = true;
	      stream.emit('prefinish');
	    }
	  }
	}

	function finishMaybe(stream, state) {
	  var need = needFinish(state);
	  if (need) {
	    prefinish(stream, state);
	    if (state.pendingcb === 0) {
	      state.finished = true;
	      stream.emit('finish');
	    }
	  }
	  return need;
	}

	function endWritable(stream, state, cb) {
	  state.ending = true;
	  finishMaybe(stream, state);
	  if (cb) {
	    if (state.finished) pna.nextTick(cb);else stream.once('finish', cb);
	  }
	  state.ended = true;
	  stream.writable = false;
	}

	function onCorkedFinish(corkReq, state, err) {
	  var entry = corkReq.entry;
	  corkReq.entry = null;
	  while (entry) {
	    var cb = entry.callback;
	    state.pendingcb--;
	    cb(err);
	    entry = entry.next;
	  }
	  if (state.corkedRequestsFree) {
	    state.corkedRequestsFree.next = corkReq;
	  } else {
	    state.corkedRequestsFree = corkReq;
	  }
	}

	Object.defineProperty(Writable.prototype, 'destroyed', {
	  get: function () {
	    if (this._writableState === undefined) {
	      return false;
	    }
	    return this._writableState.destroyed;
	  },
	  set: function (value) {
	    // we ignore the value if the stream
	    // has not been initialized yet
	    if (!this._writableState) {
	      return;
	    }

	    // backward compatibility, the user is explicitly
	    // managing destroyed
	    this._writableState.destroyed = value;
	  }
	});

	Writable.prototype.destroy = destroyImpl.destroy;
	Writable.prototype._undestroy = destroyImpl.undestroy;
	Writable.prototype._destroy = function (err, cb) {
	  this.end();
	  cb(err);
	};
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(18), __webpack_require__(49).setImmediate, (function() { return this; }())))

/***/ }),
/* 49 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(global) {var apply = Function.prototype.apply;

	// DOM APIs, for completeness

	exports.setTimeout = function() {
	  return new Timeout(apply.call(setTimeout, window, arguments), clearTimeout);
	};
	exports.setInterval = function() {
	  return new Timeout(apply.call(setInterval, window, arguments), clearInterval);
	};
	exports.clearTimeout =
	exports.clearInterval = function(timeout) {
	  if (timeout) {
	    timeout.close();
	  }
	};

	function Timeout(id, clearFn) {
	  this._id = id;
	  this._clearFn = clearFn;
	}
	Timeout.prototype.unref = Timeout.prototype.ref = function() {};
	Timeout.prototype.close = function() {
	  this._clearFn.call(window, this._id);
	};

	// Does not start the time, just sets up the members needed.
	exports.enroll = function(item, msecs) {
	  clearTimeout(item._idleTimeoutId);
	  item._idleTimeout = msecs;
	};

	exports.unenroll = function(item) {
	  clearTimeout(item._idleTimeoutId);
	  item._idleTimeout = -1;
	};

	exports._unrefActive = exports.active = function(item) {
	  clearTimeout(item._idleTimeoutId);

	  var msecs = item._idleTimeout;
	  if (msecs >= 0) {
	    item._idleTimeoutId = setTimeout(function onTimeout() {
	      if (item._onTimeout)
	        item._onTimeout();
	    }, msecs);
	  }
	};

	// setimmediate attaches itself to the global object
	__webpack_require__(50);
	// On some exotic environments, it's not clear which object `setimmeidate` was
	// able to install onto.  Search each possibility in the same order as the
	// `setimmediate` library.
	exports.setImmediate = (typeof self !== "undefined" && self.setImmediate) ||
	                       (typeof global !== "undefined" && global.setImmediate) ||
	                       (this && this.setImmediate);
	exports.clearImmediate = (typeof self !== "undefined" && self.clearImmediate) ||
	                         (typeof global !== "undefined" && global.clearImmediate) ||
	                         (this && this.clearImmediate);

	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }())))

/***/ }),
/* 50 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(global, process) {(function (global, undefined) {
	    "use strict";

	    if (global.setImmediate) {
	        return;
	    }

	    var nextHandle = 1; // Spec says greater than zero
	    var tasksByHandle = {};
	    var currentlyRunningATask = false;
	    var doc = global.document;
	    var registerImmediate;

	    function setImmediate(callback) {
	      // Callback can either be a function or a string
	      if (typeof callback !== "function") {
	        callback = new Function("" + callback);
	      }
	      // Copy function arguments
	      var args = new Array(arguments.length - 1);
	      for (var i = 0; i < args.length; i++) {
	          args[i] = arguments[i + 1];
	      }
	      // Store and register the task
	      var task = { callback: callback, args: args };
	      tasksByHandle[nextHandle] = task;
	      registerImmediate(nextHandle);
	      return nextHandle++;
	    }

	    function clearImmediate(handle) {
	        delete tasksByHandle[handle];
	    }

	    function run(task) {
	        var callback = task.callback;
	        var args = task.args;
	        switch (args.length) {
	        case 0:
	            callback();
	            break;
	        case 1:
	            callback(args[0]);
	            break;
	        case 2:
	            callback(args[0], args[1]);
	            break;
	        case 3:
	            callback(args[0], args[1], args[2]);
	            break;
	        default:
	            callback.apply(undefined, args);
	            break;
	        }
	    }

	    function runIfPresent(handle) {
	        // From the spec: "Wait until any invocations of this algorithm started before this one have completed."
	        // So if we're currently running a task, we'll need to delay this invocation.
	        if (currentlyRunningATask) {
	            // Delay by doing a setTimeout. setImmediate was tried instead, but in Firefox 7 it generated a
	            // "too much recursion" error.
	            setTimeout(runIfPresent, 0, handle);
	        } else {
	            var task = tasksByHandle[handle];
	            if (task) {
	                currentlyRunningATask = true;
	                try {
	                    run(task);
	                } finally {
	                    clearImmediate(handle);
	                    currentlyRunningATask = false;
	                }
	            }
	        }
	    }

	    function installNextTickImplementation() {
	        registerImmediate = function(handle) {
	            process.nextTick(function () { runIfPresent(handle); });
	        };
	    }

	    function canUsePostMessage() {
	        // The test against `importScripts` prevents this implementation from being installed inside a web worker,
	        // where `global.postMessage` means something completely different and can't be used for this purpose.
	        if (global.postMessage && !global.importScripts) {
	            var postMessageIsAsynchronous = true;
	            var oldOnMessage = global.onmessage;
	            global.onmessage = function() {
	                postMessageIsAsynchronous = false;
	            };
	            global.postMessage("", "*");
	            global.onmessage = oldOnMessage;
	            return postMessageIsAsynchronous;
	        }
	    }

	    function installPostMessageImplementation() {
	        // Installs an event handler on `global` for the `message` event: see
	        // * https://developer.mozilla.org/en/DOM/window.postMessage
	        // * http://www.whatwg.org/specs/web-apps/current-work/multipage/comms.html#crossDocumentMessages

	        var messagePrefix = "setImmediate$" + Math.random() + "$";
	        var onGlobalMessage = function(event) {
	            if (event.source === global &&
	                typeof event.data === "string" &&
	                event.data.indexOf(messagePrefix) === 0) {
	                runIfPresent(+event.data.slice(messagePrefix.length));
	            }
	        };

	        if (global.addEventListener) {
	            global.addEventListener("message", onGlobalMessage, false);
	        } else {
	            global.attachEvent("onmessage", onGlobalMessage);
	        }

	        registerImmediate = function(handle) {
	            global.postMessage(messagePrefix + handle, "*");
	        };
	    }

	    function installMessageChannelImplementation() {
	        var channel = new MessageChannel();
	        channel.port1.onmessage = function(event) {
	            var handle = event.data;
	            runIfPresent(handle);
	        };

	        registerImmediate = function(handle) {
	            channel.port2.postMessage(handle);
	        };
	    }

	    function installReadyStateChangeImplementation() {
	        var html = doc.documentElement;
	        registerImmediate = function(handle) {
	            // Create a <script> element; its readystatechange event will be fired asynchronously once it is inserted
	            // into the document. Do so, thus queuing up the task. Remember to clean up once it's been called.
	            var script = doc.createElement("script");
	            script.onreadystatechange = function () {
	                runIfPresent(handle);
	                script.onreadystatechange = null;
	                html.removeChild(script);
	                script = null;
	            };
	            html.appendChild(script);
	        };
	    }

	    function installSetTimeoutImplementation() {
	        registerImmediate = function(handle) {
	            setTimeout(runIfPresent, 0, handle);
	        };
	    }

	    // If supported, we should attach to the prototype of global, since that is where setTimeout et al. live.
	    var attachTo = Object.getPrototypeOf && Object.getPrototypeOf(global);
	    attachTo = attachTo && attachTo.setTimeout ? attachTo : global;

	    // Don't get fooled by e.g. browserify environments.
	    if ({}.toString.call(global.process) === "[object process]") {
	        // For Node.js before 0.9
	        installNextTickImplementation();

	    } else if (canUsePostMessage()) {
	        // For non-IE10 modern browsers
	        installPostMessageImplementation();

	    } else if (global.MessageChannel) {
	        // For web workers, where supported
	        installMessageChannelImplementation();

	    } else if (doc && "onreadystatechange" in doc.createElement("script")) {
	        // For IE 6–8
	        installReadyStateChangeImplementation();

	    } else {
	        // For older browsers
	        installSetTimeoutImplementation();
	    }

	    attachTo.setImmediate = setImmediate;
	    attachTo.clearImmediate = clearImmediate;
	}(typeof self === "undefined" ? typeof global === "undefined" ? this : global : self));

	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }()), __webpack_require__(18)))

/***/ }),
/* 51 */
/***/ (function(module, exports) {

	/* WEBPACK VAR INJECTION */(function(global) {
	/**
	 * Module exports.
	 */

	module.exports = deprecate;

	/**
	 * Mark that a method should not be used.
	 * Returns a modified function which warns once by default.
	 *
	 * If `localStorage.noDeprecation = true` is set, then it is a no-op.
	 *
	 * If `localStorage.throwDeprecation = true` is set, then deprecated functions
	 * will throw an Error when invoked.
	 *
	 * If `localStorage.traceDeprecation = true` is set, then deprecated functions
	 * will invoke `console.trace()` instead of `console.error()`.
	 *
	 * @param {Function} fn - the function to deprecate
	 * @param {String} msg - the string to print to the console when `fn` is invoked
	 * @returns {Function} a new "deprecated" version of `fn`
	 * @api public
	 */

	function deprecate (fn, msg) {
	  if (config('noDeprecation')) {
	    return fn;
	  }

	  var warned = false;
	  function deprecated() {
	    if (!warned) {
	      if (config('throwDeprecation')) {
	        throw new Error(msg);
	      } else if (config('traceDeprecation')) {
	        console.trace(msg);
	      } else {
	        console.warn(msg);
	      }
	      warned = true;
	    }
	    return fn.apply(this, arguments);
	  }

	  return deprecated;
	}

	/**
	 * Checks `localStorage` for boolean values for the given `name`.
	 *
	 * @param {String} name
	 * @returns {Boolean}
	 * @api private
	 */

	function config (name) {
	  // accessing global.localStorage can trigger a DOMException in sandboxed iframes
	  try {
	    if (!global.localStorage) return false;
	  } catch (_) {
	    return false;
	  }
	  var val = global.localStorage[name];
	  if (null == val) return false;
	  return String(val).toLowerCase() === 'true';
	}

	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }())))

/***/ }),
/* 52 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	var Buffer = __webpack_require__(41).Buffer;

	var isEncoding = Buffer.isEncoding || function (encoding) {
	  encoding = '' + encoding;
	  switch (encoding && encoding.toLowerCase()) {
	    case 'hex':case 'utf8':case 'utf-8':case 'ascii':case 'binary':case 'base64':case 'ucs2':case 'ucs-2':case 'utf16le':case 'utf-16le':case 'raw':
	      return true;
	    default:
	      return false;
	  }
	};

	function _normalizeEncoding(enc) {
	  if (!enc) return 'utf8';
	  var retried;
	  while (true) {
	    switch (enc) {
	      case 'utf8':
	      case 'utf-8':
	        return 'utf8';
	      case 'ucs2':
	      case 'ucs-2':
	      case 'utf16le':
	      case 'utf-16le':
	        return 'utf16le';
	      case 'latin1':
	      case 'binary':
	        return 'latin1';
	      case 'base64':
	      case 'ascii':
	      case 'hex':
	        return enc;
	      default:
	        if (retried) return; // undefined
	        enc = ('' + enc).toLowerCase();
	        retried = true;
	    }
	  }
	};

	// Do not cache `Buffer.isEncoding` when checking encoding names as some
	// modules monkey-patch it to support additional encodings
	function normalizeEncoding(enc) {
	  var nenc = _normalizeEncoding(enc);
	  if (typeof nenc !== 'string' && (Buffer.isEncoding === isEncoding || !isEncoding(enc))) throw new Error('Unknown encoding: ' + enc);
	  return nenc || enc;
	}

	// StringDecoder provides an interface for efficiently splitting a series of
	// buffers into a series of JS strings without breaking apart multi-byte
	// characters.
	exports.StringDecoder = StringDecoder;
	function StringDecoder(encoding) {
	  this.encoding = normalizeEncoding(encoding);
	  var nb;
	  switch (this.encoding) {
	    case 'utf16le':
	      this.text = utf16Text;
	      this.end = utf16End;
	      nb = 4;
	      break;
	    case 'utf8':
	      this.fillLast = utf8FillLast;
	      nb = 4;
	      break;
	    case 'base64':
	      this.text = base64Text;
	      this.end = base64End;
	      nb = 3;
	      break;
	    default:
	      this.write = simpleWrite;
	      this.end = simpleEnd;
	      return;
	  }
	  this.lastNeed = 0;
	  this.lastTotal = 0;
	  this.lastChar = Buffer.allocUnsafe(nb);
	}

	StringDecoder.prototype.write = function (buf) {
	  if (buf.length === 0) return '';
	  var r;
	  var i;
	  if (this.lastNeed) {
	    r = this.fillLast(buf);
	    if (r === undefined) return '';
	    i = this.lastNeed;
	    this.lastNeed = 0;
	  } else {
	    i = 0;
	  }
	  if (i < buf.length) return r ? r + this.text(buf, i) : this.text(buf, i);
	  return r || '';
	};

	StringDecoder.prototype.end = utf8End;

	// Returns only complete characters in a Buffer
	StringDecoder.prototype.text = utf8Text;

	// Attempts to complete a partial non-UTF-8 character using bytes from a Buffer
	StringDecoder.prototype.fillLast = function (buf) {
	  if (this.lastNeed <= buf.length) {
	    buf.copy(this.lastChar, this.lastTotal - this.lastNeed, 0, this.lastNeed);
	    return this.lastChar.toString(this.encoding, 0, this.lastTotal);
	  }
	  buf.copy(this.lastChar, this.lastTotal - this.lastNeed, 0, buf.length);
	  this.lastNeed -= buf.length;
	};

	// Checks the type of a UTF-8 byte, whether it's ASCII, a leading byte, or a
	// continuation byte.
	function utf8CheckByte(byte) {
	  if (byte <= 0x7F) return 0;else if (byte >> 5 === 0x06) return 2;else if (byte >> 4 === 0x0E) return 3;else if (byte >> 3 === 0x1E) return 4;
	  return -1;
	}

	// Checks at most 3 bytes at the end of a Buffer in order to detect an
	// incomplete multi-byte UTF-8 character. The total number of bytes (2, 3, or 4)
	// needed to complete the UTF-8 character (if applicable) are returned.
	function utf8CheckIncomplete(self, buf, i) {
	  var j = buf.length - 1;
	  if (j < i) return 0;
	  var nb = utf8CheckByte(buf[j]);
	  if (nb >= 0) {
	    if (nb > 0) self.lastNeed = nb - 1;
	    return nb;
	  }
	  if (--j < i) return 0;
	  nb = utf8CheckByte(buf[j]);
	  if (nb >= 0) {
	    if (nb > 0) self.lastNeed = nb - 2;
	    return nb;
	  }
	  if (--j < i) return 0;
	  nb = utf8CheckByte(buf[j]);
	  if (nb >= 0) {
	    if (nb > 0) {
	      if (nb === 2) nb = 0;else self.lastNeed = nb - 3;
	    }
	    return nb;
	  }
	  return 0;
	}

	// Validates as many continuation bytes for a multi-byte UTF-8 character as
	// needed or are available. If we see a non-continuation byte where we expect
	// one, we "replace" the validated continuation bytes we've seen so far with
	// UTF-8 replacement characters ('\ufffd'), to match v8's UTF-8 decoding
	// behavior. The continuation byte check is included three times in the case
	// where all of the continuation bytes for a character exist in the same buffer.
	// It is also done this way as a slight performance increase instead of using a
	// loop.
	function utf8CheckExtraBytes(self, buf, p) {
	  if ((buf[0] & 0xC0) !== 0x80) {
	    self.lastNeed = 0;
	    return '\ufffd'.repeat(p);
	  }
	  if (self.lastNeed > 1 && buf.length > 1) {
	    if ((buf[1] & 0xC0) !== 0x80) {
	      self.lastNeed = 1;
	      return '\ufffd'.repeat(p + 1);
	    }
	    if (self.lastNeed > 2 && buf.length > 2) {
	      if ((buf[2] & 0xC0) !== 0x80) {
	        self.lastNeed = 2;
	        return '\ufffd'.repeat(p + 2);
	      }
	    }
	  }
	}

	// Attempts to complete a multi-byte UTF-8 character using bytes from a Buffer.
	function utf8FillLast(buf) {
	  var p = this.lastTotal - this.lastNeed;
	  var r = utf8CheckExtraBytes(this, buf, p);
	  if (r !== undefined) return r;
	  if (this.lastNeed <= buf.length) {
	    buf.copy(this.lastChar, p, 0, this.lastNeed);
	    return this.lastChar.toString(this.encoding, 0, this.lastTotal);
	  }
	  buf.copy(this.lastChar, p, 0, buf.length);
	  this.lastNeed -= buf.length;
	}

	// Returns all complete UTF-8 characters in a Buffer. If the Buffer ended on a
	// partial character, the character's bytes are buffered until the required
	// number of bytes are available.
	function utf8Text(buf, i) {
	  var total = utf8CheckIncomplete(this, buf, i);
	  if (!this.lastNeed) return buf.toString('utf8', i);
	  this.lastTotal = total;
	  var end = buf.length - (total - this.lastNeed);
	  buf.copy(this.lastChar, 0, end);
	  return buf.toString('utf8', i, end);
	}

	// For UTF-8, a replacement character for each buffered byte of a (partial)
	// character needs to be added to the output.
	function utf8End(buf) {
	  var r = buf && buf.length ? this.write(buf) : '';
	  if (this.lastNeed) return r + '\ufffd'.repeat(this.lastTotal - this.lastNeed);
	  return r;
	}

	// UTF-16LE typically needs two bytes per character, but even if we have an even
	// number of bytes available, we need to check if we end on a leading/high
	// surrogate. In that case, we need to wait for the next two bytes in order to
	// decode the last character properly.
	function utf16Text(buf, i) {
	  if ((buf.length - i) % 2 === 0) {
	    var r = buf.toString('utf16le', i);
	    if (r) {
	      var c = r.charCodeAt(r.length - 1);
	      if (c >= 0xD800 && c <= 0xDBFF) {
	        this.lastNeed = 2;
	        this.lastTotal = 4;
	        this.lastChar[0] = buf[buf.length - 2];
	        this.lastChar[1] = buf[buf.length - 1];
	        return r.slice(0, -1);
	      }
	    }
	    return r;
	  }
	  this.lastNeed = 1;
	  this.lastTotal = 2;
	  this.lastChar[0] = buf[buf.length - 1];
	  return buf.toString('utf16le', i, buf.length - 1);
	}

	// For UTF-16LE we do not explicitly append special replacement characters if we
	// end on a partial character, we simply let v8 handle that.
	function utf16End(buf) {
	  var r = buf && buf.length ? this.write(buf) : '';
	  if (this.lastNeed) {
	    var end = this.lastTotal - this.lastNeed;
	    return r + this.lastChar.toString('utf16le', 0, end);
	  }
	  return r;
	}

	function base64Text(buf, i) {
	  var n = (buf.length - i) % 3;
	  if (n === 0) return buf.toString('base64', i);
	  this.lastNeed = 3 - n;
	  this.lastTotal = 3;
	  if (n === 1) {
	    this.lastChar[0] = buf[buf.length - 1];
	  } else {
	    this.lastChar[0] = buf[buf.length - 2];
	    this.lastChar[1] = buf[buf.length - 1];
	  }
	  return buf.toString('base64', i, buf.length - n);
	}

	function base64End(buf) {
	  var r = buf && buf.length ? this.write(buf) : '';
	  if (this.lastNeed) return r + this.lastChar.toString('base64', 0, 3 - this.lastNeed);
	  return r;
	}

	// Pass bytes on through for single-byte encodings (e.g. ascii, latin1, hex)
	function simpleWrite(buf) {
	  return buf.toString(this.encoding);
	}

	function simpleEnd(buf) {
	  return buf && buf.length ? this.write(buf) : '';
	}

/***/ }),
/* 53 */
/***/ (function(module, exports, __webpack_require__) {

	// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	// a transform stream is a readable/writable stream where you do
	// something with the data.  Sometimes it's called a "filter",
	// but that's not a great name for it, since that implies a thing where
	// some bits pass through, and others are simply ignored.  (That would
	// be a valid example of a transform, of course.)
	//
	// While the output is causally related to the input, it's not a
	// necessarily symmetric or synchronous transformation.  For example,
	// a zlib stream might take multiple plain-text writes(), and then
	// emit a single compressed chunk some time in the future.
	//
	// Here's how this works:
	//
	// The Transform stream has all the aspects of the readable and writable
	// stream classes.  When you write(chunk), that calls _write(chunk,cb)
	// internally, and returns false if there's a lot of pending writes
	// buffered up.  When you call read(), that calls _read(n) until
	// there's enough pending readable data buffered up.
	//
	// In a transform stream, the written data is placed in a buffer.  When
	// _read(n) is called, it transforms the queued up data, calling the
	// buffered _write cb's as it consumes chunks.  If consuming a single
	// written chunk would result in multiple output chunks, then the first
	// outputted bit calls the readcb, and subsequent chunks just go into
	// the read buffer, and will cause it to emit 'readable' if necessary.
	//
	// This way, back-pressure is actually determined by the reading side,
	// since _read has to be called to start processing a new chunk.  However,
	// a pathological inflate type of transform can cause excessive buffering
	// here.  For example, imagine a stream where every byte of input is
	// interpreted as an integer from 0-255, and then results in that many
	// bytes of output.  Writing the 4 bytes {ff,ff,ff,ff} would result in
	// 1kb of data being output.  In this case, you could write a very small
	// amount of input, and end up with a very large amount of output.  In
	// such a pathological inflating mechanism, there'd be no way to tell
	// the system to stop doing the transform.  A single 4MB write could
	// cause the system to run out of memory.
	//
	// However, even in such a pathological case, only a single written chunk
	// would be consumed, and then the rest would wait (un-transformed) until
	// the results of the previous transformed chunk were consumed.

	'use strict';

	module.exports = Transform;

	var Duplex = __webpack_require__(47);

	/*<replacement>*/
	var util = __webpack_require__(42);
	util.inherits = __webpack_require__(35);
	/*</replacement>*/

	util.inherits(Transform, Duplex);

	function afterTransform(er, data) {
	  var ts = this._transformState;
	  ts.transforming = false;

	  var cb = ts.writecb;

	  if (!cb) {
	    return this.emit('error', new Error('write callback called multiple times'));
	  }

	  ts.writechunk = null;
	  ts.writecb = null;

	  if (data != null) // single equals check for both `null` and `undefined`
	    this.push(data);

	  cb(er);

	  var rs = this._readableState;
	  rs.reading = false;
	  if (rs.needReadable || rs.length < rs.highWaterMark) {
	    this._read(rs.highWaterMark);
	  }
	}

	function Transform(options) {
	  if (!(this instanceof Transform)) return new Transform(options);

	  Duplex.call(this, options);

	  this._transformState = {
	    afterTransform: afterTransform.bind(this),
	    needTransform: false,
	    transforming: false,
	    writecb: null,
	    writechunk: null,
	    writeencoding: null
	  };

	  // start out asking for a readable event once data is transformed.
	  this._readableState.needReadable = true;

	  // we have implemented the _read method, and done the other things
	  // that Readable wants before the first _read call, so unset the
	  // sync guard flag.
	  this._readableState.sync = false;

	  if (options) {
	    if (typeof options.transform === 'function') this._transform = options.transform;

	    if (typeof options.flush === 'function') this._flush = options.flush;
	  }

	  // When the writable side finishes, then flush out anything remaining.
	  this.on('prefinish', prefinish);
	}

	function prefinish() {
	  var _this = this;

	  if (typeof this._flush === 'function') {
	    this._flush(function (er, data) {
	      done(_this, er, data);
	    });
	  } else {
	    done(this, null, null);
	  }
	}

	Transform.prototype.push = function (chunk, encoding) {
	  this._transformState.needTransform = false;
	  return Duplex.prototype.push.call(this, chunk, encoding);
	};

	// This is the part where you do stuff!
	// override this function in implementation classes.
	// 'chunk' is an input chunk.
	//
	// Call `push(newChunk)` to pass along transformed output
	// to the readable side.  You may call 'push' zero or more times.
	//
	// Call `cb(err)` when you are done with this chunk.  If you pass
	// an error, then that'll put the hurt on the whole operation.  If you
	// never call cb(), then you'll never get another chunk.
	Transform.prototype._transform = function (chunk, encoding, cb) {
	  throw new Error('_transform() is not implemented');
	};

	Transform.prototype._write = function (chunk, encoding, cb) {
	  var ts = this._transformState;
	  ts.writecb = cb;
	  ts.writechunk = chunk;
	  ts.writeencoding = encoding;
	  if (!ts.transforming) {
	    var rs = this._readableState;
	    if (ts.needTransform || rs.needReadable || rs.length < rs.highWaterMark) this._read(rs.highWaterMark);
	  }
	};

	// Doesn't matter what the args are here.
	// _transform does all the work.
	// That we got here means that the readable side wants more data.
	Transform.prototype._read = function (n) {
	  var ts = this._transformState;

	  if (ts.writechunk !== null && ts.writecb && !ts.transforming) {
	    ts.transforming = true;
	    this._transform(ts.writechunk, ts.writeencoding, ts.afterTransform);
	  } else {
	    // mark that we need a transform, so that any data that comes in
	    // will get processed, now that we've asked for it.
	    ts.needTransform = true;
	  }
	};

	Transform.prototype._destroy = function (err, cb) {
	  var _this2 = this;

	  Duplex.prototype._destroy.call(this, err, function (err2) {
	    cb(err2);
	    _this2.emit('close');
	  });
	};

	function done(stream, er, data) {
	  if (er) return stream.emit('error', er);

	  if (data != null) // single equals check for both `null` and `undefined`
	    stream.push(data);

	  // if there's nothing in the write buffer, then that means
	  // that nothing more will ever be provided
	  if (stream._writableState.length) throw new Error('Calling transform done when ws.length != 0');

	  if (stream._transformState.transforming) throw new Error('Calling transform done when still transforming');

	  return stream.push(null);
	}

/***/ }),
/* 54 */
/***/ (function(module, exports, __webpack_require__) {

	// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	// a passthrough stream.
	// basically just the most minimal sort of Transform stream.
	// Every written chunk gets output as-is.

	'use strict';

	module.exports = PassThrough;

	var Transform = __webpack_require__(53);

	/*<replacement>*/
	var util = __webpack_require__(42);
	util.inherits = __webpack_require__(35);
	/*</replacement>*/

	util.inherits(PassThrough, Transform);

	function PassThrough(options) {
	  if (!(this instanceof PassThrough)) return new PassThrough(options);

	  Transform.call(this, options);
	}

	PassThrough.prototype._transform = function (chunk, encoding, cb) {
	  cb(null, chunk);
	};

/***/ }),
/* 55 */
/***/ (function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__(48);


/***/ }),
/* 56 */
/***/ (function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__(47);


/***/ }),
/* 57 */
/***/ (function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__(36).Transform


/***/ }),
/* 58 */
/***/ (function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__(36).PassThrough


/***/ }),
/* 59 */
/***/ (function(module, exports) {

	exports['aes-128-ecb'] = {
	  cipher: 'AES',
	  key: 128,
	  iv: 0,
	  mode: 'ECB',
	  type: 'block'
	};
	exports['aes-192-ecb'] = {
	  cipher: 'AES',
	  key: 192,
	  iv: 0,
	  mode: 'ECB',
	  type: 'block'
	};
	exports['aes-256-ecb'] = {
	  cipher: 'AES',
	  key: 256,
	  iv: 0,
	  mode: 'ECB',
	  type: 'block'
	};
	exports['aes-128-cbc'] = {
	  cipher: 'AES',
	  key: 128,
	  iv: 16,
	  mode: 'CBC',
	  type: 'block'
	};
	exports['aes-192-cbc'] = {
	  cipher: 'AES',
	  key: 192,
	  iv: 16,
	  mode: 'CBC',
	  type: 'block'
	};
	exports['aes-256-cbc'] = {
	  cipher: 'AES',
	  key: 256,
	  iv: 16,
	  mode: 'CBC',
	  type: 'block'
	};
	exports['aes128'] = exports['aes-128-cbc'];
	exports['aes192'] = exports['aes-192-cbc'];
	exports['aes256'] = exports['aes-256-cbc'];
	exports['aes-128-cfb'] = {
	  cipher: 'AES',
	  key: 128,
	  iv: 16,
	  mode: 'CFB',
	  type: 'stream'
	};
	exports['aes-192-cfb'] = {
	  cipher: 'AES',
	  key: 192,
	  iv: 16,
	  mode: 'CFB',
	  type: 'stream'
	};
	exports['aes-256-cfb'] = {
	  cipher: 'AES',
	  key: 256,
	  iv: 16,
	  mode: 'CFB',
	  type: 'stream'
	};
	exports['aes-128-ofb'] = {
	  cipher: 'AES',
	  key: 128,
	  iv: 16,
	  mode: 'OFB',
	  type: 'stream'
	};
	exports['aes-192-ofb'] = {
	  cipher: 'AES',
	  key: 192,
	  iv: 16,
	  mode: 'OFB',
	  type: 'stream'
	};
	exports['aes-256-ofb'] = {
	  cipher: 'AES',
	  key: 256,
	  iv: 16,
	  mode: 'OFB',
	  type: 'stream'
	};
	exports['aes-128-ctr'] = {
	  cipher: 'AES',
	  key: 128,
	  iv: 16,
	  mode: 'CTR',
	  type: 'stream'
	};
	exports['aes-192-ctr'] = {
	  cipher: 'AES',
	  key: 192,
	  iv: 16,
	  mode: 'CTR',
	  type: 'stream'
	};
	exports['aes-256-ctr'] = {
	  cipher: 'AES',
	  key: 256,
	  iv: 16,
	  mode: 'CTR',
	  type: 'stream'
	};

/***/ }),
/* 60 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {
	module.exports = function (crypto, password, keyLen, ivLen) {
	  keyLen = keyLen/8;
	  ivLen = ivLen || 0;
	  var ki = 0;
	  var ii = 0;
	  var key = new Buffer(keyLen);
	  var iv = new Buffer(ivLen);
	  var addmd = 0;
	  var md, md_buf;
	  var i;
	  while (true) {
	    md = crypto.createHash('md5');
	    if(addmd++ > 0) {
	       md.update(md_buf);
	    }
	    md.update(password);
	    md_buf = md.digest();
	    i = 0;
	    if(keyLen > 0) {
	      while(true) {
	        if(keyLen === 0) {
	          break;
	        }
	        if(i === md_buf.length) {
	          break;
	        }
	        key[ki++] = md_buf[i];
	        keyLen--;
	        i++;
	       }
	    }
	    if(ivLen > 0 && i !== md_buf.length) {
	      while(true) {
	        if(ivLen === 0) {
	          break;
	        }
	        if(i === md_buf.length) {
	          break;
	        }
	       iv[ii++] = md_buf[i];
	       ivLen--;
	       i++;
	     }
	   }
	   if(keyLen === 0 && ivLen === 0) {
	      break;
	    }
	  }
	  for(i=0;i<md_buf.length;i++) {
	    md_buf[i] = 0;
	  }
	  return {
	    key: key,
	    iv: iv
	  };
	};
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 61 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {var aes = __webpack_require__(31);
	var Transform = __webpack_require__(32);
	var inherits = __webpack_require__(35);

	inherits(StreamCipher, Transform);
	module.exports = StreamCipher;
	function StreamCipher(mode, key, iv, decrypt) {
	  if (!(this instanceof StreamCipher)) {
	    return new StreamCipher(mode, key, iv);
	  }
	  Transform.call(this);
	  this._cipher = new aes.AES(key);
	  this._prev = new Buffer(iv.length);
	  this._cache = new Buffer('');
	  this._secCache = new Buffer('');
	  this._decrypt = decrypt;
	  iv.copy(this._prev);
	  this._mode = mode;
	}
	StreamCipher.prototype._transform = function (chunk, _, next) {
	  next(null, this._mode.encrypt(this, chunk, this._decrypt));
	};
	StreamCipher.prototype._flush = function (next) {
	  this._cipher.scrub();
	  next();
	};
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 62 */
/***/ (function(module, exports) {

	exports.encrypt = function (self, block) {
	  return self._cipher.encryptBlock(block);
	};
	exports.decrypt = function (self, block) {
	  return self._cipher.decryptBlock(block);
	};

/***/ }),
/* 63 */
/***/ (function(module, exports, __webpack_require__) {

	var xor = __webpack_require__(64);
	exports.encrypt = function (self, block) {
	  var data = xor(block, self._prev);
	  self._prev = self._cipher.encryptBlock(data);
	  return self._prev;
	};
	exports.decrypt = function (self, block) {
	  var pad = self._prev;
	  self._prev = block;
	  var out = self._cipher.decryptBlock(block);
	  return xor(out, pad);
	};

/***/ }),
/* 64 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {module.exports = xor;
	function xor(a, b) {
	  var len = Math.min(a.length, b.length);
	  var out = new Buffer(len);
	  var i = -1;
	  while (++i < len) {
	    out.writeUInt8(a[i] ^ b[i], i);
	  }
	  return out;
	}
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 65 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {var xor = __webpack_require__(64);
	exports.encrypt = function (self, data, decrypt) {
	  var out = new Buffer('');
	  var len;
	  while (data.length) {
	    if (self._cache.length === 0) {
	      self._cache = self._cipher.encryptBlock(self._prev);
	      self._prev = new Buffer('');
	    }
	    if (self._cache.length <= data.length) {
	      len = self._cache.length;
	      out = Buffer.concat([out, encryptStart(self, data.slice(0, len), decrypt)]);
	      data = data.slice(len);
	    } else {
	      out = Buffer.concat([out, encryptStart(self, data, decrypt)]);
	      break;
	    }
	  }
	  return out;
	};
	function encryptStart(self, data, decrypt) {
	  var len = data.length;
	  var out = xor(data, self._cache);
	  self._cache = self._cache.slice(len);
	  self._prev = Buffer.concat([self._prev, decrypt?data:out]);
	  return out;
	}
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 66 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {var xor = __webpack_require__(64);
	function getBlock(self) {
	  self._prev = self._cipher.encryptBlock(self._prev);
	  return self._prev;
	}
	exports.encrypt = function (self, chunk) {
	  while (self._cache.length < chunk.length) {
	    self._cache = Buffer.concat([self._cache, getBlock(self)]);
	  }
	  var pad = self._cache.slice(0, chunk.length);
	  self._cache = self._cache.slice(chunk.length);
	  return xor(chunk, pad);
	};
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 67 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {var xor = __webpack_require__(64);
	function getBlock(self) {
	  var out = self._cipher.encryptBlock(self._prev);
	  incr32(self._prev);
	  return out;
	}
	exports.encrypt = function (self, chunk) {
	  while (self._cache.length < chunk.length) {
	    self._cache = Buffer.concat([self._cache, getBlock(self)]);
	  }
	  var pad = self._cache.slice(0, chunk.length);
	  self._cache = self._cache.slice(chunk.length);
	  return xor(chunk, pad);
	};
	function incr32(iv) {
	  var len = iv.length;
	  var item;
	  while (len--) {
	    item = iv.readUInt8(len);
	    if (item === 255) {
	      iv.writeUInt8(0, len);
	    } else {
	      item++;
	      iv.writeUInt8(item, len);
	      break;
	    }
	  }
	}
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 68 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {var aes = __webpack_require__(31);
	var Transform = __webpack_require__(32);
	var inherits = __webpack_require__(35);
	var modes = __webpack_require__(59);
	var StreamCipher = __webpack_require__(61);
	var ebtk = __webpack_require__(60);

	inherits(Decipher, Transform);
	function Decipher(mode, key, iv) {
	  if (!(this instanceof Decipher)) {
	    return new Decipher(mode, key, iv);
	  }
	  Transform.call(this);
	  this._cache = new Splitter();
	  this._last = void 0;
	  this._cipher = new aes.AES(key);
	  this._prev = new Buffer(iv.length);
	  iv.copy(this._prev);
	  this._mode = mode;
	}
	Decipher.prototype._transform = function (data, _, next) {
	  this._cache.add(data);
	  var chunk;
	  var thing;
	  while ((chunk = this._cache.get())) {
	    thing = this._mode.decrypt(this, chunk);
	    this.push(thing);
	  }
	  next();
	};
	Decipher.prototype._flush = function (next) {
	  var chunk = this._cache.flush();
	  if (!chunk) {
	    return next;
	  }

	  this.push(unpad(this._mode.decrypt(this, chunk)));

	  next();
	};

	function Splitter() {
	   if (!(this instanceof Splitter)) {
	    return new Splitter();
	  }
	  this.cache = new Buffer('');
	}
	Splitter.prototype.add = function (data) {
	  this.cache = Buffer.concat([this.cache, data]);
	};

	Splitter.prototype.get = function () {
	  if (this.cache.length > 16) {
	    var out = this.cache.slice(0, 16);
	    this.cache = this.cache.slice(16);
	    return out;
	  }
	  return null;
	};
	Splitter.prototype.flush = function () {
	  if (this.cache.length) {
	    return this.cache;
	  }
	};
	function unpad(last) {
	  var padded = last[15];
	  if (padded === 16) {
	    return;
	  }
	  return last.slice(0, 16 - padded);
	}

	var modelist = {
	  ECB: __webpack_require__(62),
	  CBC: __webpack_require__(63),
	  CFB: __webpack_require__(65),
	  OFB: __webpack_require__(66),
	  CTR: __webpack_require__(67)
	};

	module.exports = function (crypto) {
	  function createDecipheriv(suite, password, iv) {
	    var config = modes[suite];
	    if (!config) {
	      throw new TypeError('invalid suite type');
	    }
	    if (typeof iv === 'string') {
	      iv = new Buffer(iv);
	    }
	    if (typeof password === 'string') {
	      password = new Buffer(password);
	    }
	    if (password.length !== config.key/8) {
	      throw new TypeError('invalid key length ' + password.length);
	    }
	    if (iv.length !== config.iv) {
	      throw new TypeError('invalid iv length ' + iv.length);
	    }
	    if (config.type === 'stream') {
	      return new StreamCipher(modelist[config.mode], password, iv, true);
	    }
	    return new Decipher(modelist[config.mode], password, iv);
	  }

	  function createDecipher (suite, password) {
	    var config = modes[suite];
	    if (!config) {
	      throw new TypeError('invalid suite type');
	    }
	    var keys = ebtk(crypto, password, config.key, config.iv);
	    return createDecipheriv(suite, keys.key, keys.iv);
	  }
	  return {
	    createDecipher: createDecipher,
	    createDecipheriv: createDecipheriv
	  };
	};

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 69 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/


	//SCRIPT START

	var Constants = {
	    MediaTypes: {
	        Any: "*/*",
	        ImageJpeg: "image/jpeg",
	        ImagePng: "image/png",
	        Javascript: "application/x-javascript",
	        Json: "application/json",
	        OctetStream: "application/octet-stream",
	        QueryJson: "application/query+json",
	        SQL: "application/sql",
	        TextHtml: "text/html",
	        TextPlain: "text/plain",
	        Xml: "application/xml"
	    },

	    HttpMethods: {
	        Get: "GET",
	        Post: "POST",
	        Put: "PUT",
	        Delete: "DELETE",
	        Head: "HEAD",
	        Options: "OPTIONS"
	    },

	    HttpHeaders: {
	        Authorization: "authorization",
	        ETag: "etag",
	        MethodOverride: "X-HTTP-Method",
	        Slug: "Slug",
	        ContentType: "Content-Type",
	        LastModified: "Last-Modified",
	        ContentEncoding: "Content-Encoding",
	        CharacterSet: "CharacterSet",
	        UserAgent: "User-Agent",
	        IfModifiedSince: "If-Modified-Since",
	        IfMatch: "If-Match",
	        IfNoneMatch: "If-None-Match",
	        ContentLength: "Content-Length",
	        AcceptEncoding: "Accept-Encoding",
	        KeepAlive: "Keep-Alive",
	        CacheControl: "Cache-Control",
	        TransferEncoding: "Transfer-Encoding",
	        ContentLanguage: "Content-Language",
	        ContentLocation: "Content-Location",
	        ContentMd5: "Content-Md5",
	        ContentRange: "Content-Range",
	        Accept: "Accept",
	        AcceptCharset: "Accept-Charset",
	        AcceptLanguage: "Accept-Language",
	        IfRange: "If-Range",
	        IfUnmodifiedSince: "If-Unmodified-Since",
	        MaxForwards: "Max-Forwards",
	        ProxyAuthorization: "Proxy-Authorization",
	        AcceptRanges: "Accept-Ranges",
	        ProxyAuthenticate: "Proxy-Authenticate",
	        RetryAfter: "Retry-After",
	        SetCookie: "Set-Cookie",
	        WwwAuthenticate: "Www-Authenticate",
	        Origin: "Origin",
	        Host: "Host",
	        AccessControlAllowOrigin: "Access-Control-Allow-Origin",
	        AccessControlAllowHeaders: "Access-Control-Allow-Headers",
	        KeyValueEncodingFormat: "application/x-www-form-urlencoded",
	        WrapAssertionFormat: "wrap_assertion_format",
	        WrapAssertion: "wrap_assertion",
	        WrapScope: "wrap_scope",
	        SimpleToken: "SWT",
	        HttpDate: "date",
	        Prefer: "Prefer",
	        Location: "Location",
	        Referer: "referer",

	        // Query
	        Query: "x-ms-documentdb-query",
	        IsQuery: "x-ms-documentdb-isquery",

	        // Our custom Azure Cosmos DB headers
	        Continuation: "x-ms-continuation",
	        PageSize: "x-ms-max-item-count",

	        // Request sender generated. Simply echoed by backend.
	        ActivityId: "x-ms-activity-id",
	        PreTriggerInclude: "x-ms-documentdb-pre-trigger-include",
	        PreTriggerExclude: "x-ms-documentdb-pre-trigger-exclude",
	        PostTriggerInclude: "x-ms-documentdb-post-trigger-include",
	        PostTriggerExclude: "x-ms-documentdb-post-trigger-exclude",
	        IndexingDirective: "x-ms-indexing-directive",
	        SessionToken: "x-ms-session-token",
	        ConsistencyLevel: "x-ms-consistency-level",
	        XDate: "x-ms-date",
	        CollectionPartitionInfo: "x-ms-collection-partition-info",
	        CollectionServiceInfo: "x-ms-collection-service-info",
	        RetryAfterInMilliseconds: "x-ms-retry-after-ms",
	        IsFeedUnfiltered: "x-ms-is-feed-unfiltered",
	        ResourceTokenExpiry: "x-ms-documentdb-expiry-seconds",
	        EnableScanInQuery: "x-ms-documentdb-query-enable-scan",
	        EmitVerboseTracesInQuery: "x-ms-documentdb-query-emit-traces",
	        EnableCrossPartitionQuery: "x-ms-documentdb-query-enablecrosspartition",
	        ParallelizeCrossPartitionQuery: "x-ms-documentdb-query-parallelizecrosspartitionquery",

	        // Version headers and values
	        Version: "x-ms-version",

	        //Owner name
	        OwnerFullName: "x-ms-alt-content-path",

	        // Owner ID used for name based request in session token.
	        OwnerId: "x-ms-content-path",

	        // Partition Key
	        PartitionKey: "x-ms-documentdb-partitionkey",
	        PartitionKeyRangeID: 'x-ms-documentdb-partitionkeyrangeid',

	        //Quota Info
	        MaxEntityCount: "x-ms-root-entity-max-count",
	        CurrentEntityCount: "x-ms-root-entity-current-count",
	        CollectionQuotaInMb: "x-ms-collection-quota-mb",
	        CollectionCurrentUsageInMb: "x-ms-collection-usage-mb",
	        MaxMediaStorageUsageInMB: "x-ms-max-media-storage-usage-mb",
	        CurrentMediaStorageUsageInMB: "x-ms-media-storage-usage-mb",
	        RequestCharge: "x-ms-request-charge",
	        PopulateQuotaInfo: "x-ms-documentdb-populatequotainfo",
	        MaxResourceQuota: "x-ms-resource-quota",

	        // Offer header
	        OfferType: "x-ms-offer-type",
	        OfferThroughput: "x-ms-offer-throughput",

	        // Custom RUs/minute headers
	        DisableRUPerMinuteUsage: "x-ms-documentdb-disable-ru-per-minute-usage",
	        IsRUPerMinuteUsed: "x-ms-documentdb-is-ru-per-minute-used",
	        OfferIsRUPerMinuteThroughputEnabled: "x-ms-offer-is-ru-per-minute-throughput-enabled",

	        // Index progress headers
	        IndexTransformationProgress: "x-ms-documentdb-collection-index-transformation-progress",
	        LazyIndexingProgress: "x-ms-documentdb-collection-lazy-indexing-progress",

	        // Upsert header
	        IsUpsert: "x-ms-documentdb-is-upsert",

	        // Sub status of the error
	        SubStatus: "x-ms-substatus",

	        // StoredProcedure related headers
	        EnableScriptLogging: "x-ms-documentdb-script-enable-logging",
	        ScriptLogResults: "x-ms-documentdb-script-log-results"
	    },

	    // GlobalDB related constants
	    WritableLocations: 'writableLocations',
	    ReadableLocations: 'readableLocations',
	    Name: 'name',
	    DatabaseAccountEndpoint: 'databaseAccountEndpoint',

	    // Client generated retry count response header
	    ThrottleRetryCount: "x-ms-throttle-retry-count",
	    ThrottleRetryWaitTimeInMs: "x-ms-throttle-retry-wait-time-ms",

	    CurrentVersion: "2017-11-15",

	    SDKName: "documentdb-nodejs-sdk",
	    SDKVersion: "1.14.2",

	    DefaultPrecisions: {
	        DefaultNumberHashPrecision: 3,
	        DefaultNumberRangePrecision: -1,
	        DefaultStringHashPrecision: 3,
	        DefaultStringRangePrecision: -1
	    },

	    ConsistentHashRing: {
	        DefaultVirtualNodesPerCollection: 128
	    },

	    RegularExpressions: {
	        TrimLeftSlashes: new RegExp("^[/]+"),
	        TrimRightSlashes: new RegExp("[/]+$"),
	        IllegalResourceIdCharacters: new RegExp("[/\\\\?#]")
	    },

	    Quota: {
	        CollectionSize: "collectionSize"
	    },

	    Path: {
	        DatabasesPathSegment: "dbs",
	        CollectionsPathSegment: "colls",
	        UsersPathSegment: "users",
	        DocumentsPathSegment: "docs",
	        PermissionsPathSegment: "permissions",
	        StoredProceduresPathSegment: "sprocs",
	        TriggersPathSegment: "triggers",
	        UserDefinedFunctionsPathSegment: "udfs",
	        ConflictsPathSegment: "conflicts",
	        AttachmentsPathSegment: "attachments",
	        PartitionKeyRangesPathSegment: "pkranges",
	        SchemasPathSegment: "schemas"
	    },

	    OperationTypes: {
	        Create: "create",
	        Replace: "replace",
	        Upsert: "upsert",
	        Delete: "delete",
	        Read: "read",
	        Query: "query",
	    },

	};

	//SCRIPT END

	if (true) {
	    module.exports = Constants;
	}


/***/ }),
/* 70 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer, process) {/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Constants = __webpack_require__(69);
	var os = __webpack_require__(71);
	var util = __webpack_require__(17);
	var semaphore = __webpack_require__(72);
	var Platform = {
	    /** @ignore */
	    getPlatformDefaultHeaders: function () {
	        var defaultHeaders = {};
	        defaultHeaders[Constants.HttpHeaders.UserAgent] = Platform.getUserAgent();
	        return defaultHeaders;
	    },
	    /** @ignore */
	    getDecodedDataLength: function (encodedData) {
	        var buffer = new Buffer(encodedData, "base64");
	        return buffer.length;
	    },
	    /** @ignore */
	    getUserAgent: function () {
	        // gets the user agent in the following format
	        // "{OSName}/{OSVersion} Nodejs/{NodejsVersion} documentdb-nodejs-sdk/{SDKVersion}"
	        // for example:
	        // "linux/3.4.0+ Nodejs/v0.10.25 documentdb-nodejs-sdk/1.10.0"
	        // "win32/10.0.14393 Nodejs/v4.4.7 documentdb-nodejs-sdk/1.10.0"
	        var osName = Platform._getSafeUserAgentSegmentInfo(os.platform());
	        var osVersion = Platform._getSafeUserAgentSegmentInfo(os.release());
	        var nodejsVersion = Platform._getSafeUserAgentSegmentInfo(process.version);

	        var userAgent = util.format("%s/%s Nodejs/%s %s/%s", osName, osVersion,
	            nodejsVersion,
	            Constants.SDKName, Constants.SDKVersion);

	        return userAgent;
	    },
	    /** @ignore */
	    _getSafeUserAgentSegmentInfo: function (s) {
	        // catch null, undefined, etc
	        if (typeof (s) !== 'string') {
	            s = "unknown";
	        }
	        // remove all white spaces
	        s = s.replace(/\s+/g, '');
	        if (!s) {
	            s = "unknown";
	        }
	        return s
	    }
	}

	if (true) {
	    module.exports = Platform;
	}
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer, __webpack_require__(18)))

/***/ }),
/* 71 */
/***/ (function(module, exports) {

	exports.endianness = function () { return 'LE' };

	exports.hostname = function () {
	    if (typeof location !== 'undefined') {
	        return location.hostname
	    }
	    else return '';
	};

	exports.loadavg = function () { return [] };

	exports.uptime = function () { return 0 };

	exports.freemem = function () {
	    return Number.MAX_VALUE;
	};

	exports.totalmem = function () {
	    return Number.MAX_VALUE;
	};

	exports.cpus = function () { return [] };

	exports.type = function () { return 'Browser' };

	exports.release = function () {
	    if (typeof navigator !== 'undefined') {
	        return navigator.appVersion;
	    }
	    return '';
	};

	exports.networkInterfaces
	= exports.getNetworkInterfaces
	= function () { return {} };

	exports.arch = function () { return 'javascript' };

	exports.platform = function () { return 'browser' };

	exports.tmpdir = exports.tmpDir = function () {
	    return '/tmp';
	};

	exports.EOL = '\n';


/***/ }),
/* 72 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(process) {;(function(global) {

	'use strict';

	function semaphore(capacity) {
		var semaphore = {
			capacity: capacity || 1,
			current: 0,
			queue: [],
			firstHere: false,

			take: function() {
				if (semaphore.firstHere === false) {
	        			semaphore.current++;
	        			semaphore.firstHere = true;
	        			var isFirst = 1;
	      			} else {
	        			var isFirst = 0;
	      			}
				var item = { n: 1 };

				if (typeof arguments[0] == 'function') {
					item.task = arguments[0];
				} else {
					item.n = arguments[0];
				}

				if (arguments.length >= 2)  {
					if (typeof arguments[1] == 'function') item.task = arguments[1];
					else item.n = arguments[1];
				}

				var task = item.task;
				item.task = function() { task(semaphore.leave); };

				if (semaphore.current + item.n - isFirst > semaphore.capacity) {
	        			if (isFirst === 1) {
	        				semaphore.current--;
	        				semaphore.firstHere = false;
	        			}
					return semaphore.queue.push(item);
				}

				semaphore.current += item.n - isFirst;
				item.task(semaphore.leave);
	      			if (isFirst === 1) semaphore.firstHere = false;
			},

			leave: function(n) {
				n = n || 1;

				semaphore.current -= n;

				if (!semaphore.queue.length) {
					if (semaphore.current < 0) {
						throw new Error('leave called too many times.');
					}

					return;
				}

				var item = semaphore.queue[0];

				if (item.n + semaphore.current > semaphore.capacity) {
					return;
				}

				semaphore.queue.shift();
				semaphore.current += item.n;

				if (typeof process != 'undefined' && process && typeof process.nextTick == 'function') {
					// node.js and the like
					process.nextTick(item.task);
				} else {
					setTimeout(item.task,0);
				}
			}
		};

		return semaphore;
	};

	if (true) {
	    // node export
	    module.exports = semaphore;
	} else if (typeof define === 'function' && define.amd) {
	    // amd export
	    define(function () {
	        return semaphore;
	    });
	} else {
	    // browser global
	    global.semaphore = semaphore;
	}
	}(this));

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(18)))

/***/ }),
/* 73 */
/***/ (function(module, exports, __webpack_require__) {

	var http = __webpack_require__(74);

	var https = module.exports;

	for (var key in http) {
	    if (http.hasOwnProperty(key)) https[key] = http[key];
	};

	https.request = function (params, cb) {
	    if (!params) params = {};
	    params.scheme = 'https';
	    params.protocol = 'https:';
	    return http.request.call(this, params, cb);
	}


/***/ }),
/* 74 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(global) {var ClientRequest = __webpack_require__(75)
	var IncomingMessage = __webpack_require__(77)
	var extend = __webpack_require__(92)
	var statusCodes = __webpack_require__(93)
	var url = __webpack_require__(94)

	var http = exports

	http.request = function (opts, cb) {
		if (typeof opts === 'string')
			opts = url.parse(opts)
		else
			opts = extend(opts)

		// Normally, the page is loaded from http or https, so not specifying a protocol
		// will result in a (valid) protocol-relative url. However, this won't work if
		// the protocol is something else, like 'file:'
		var defaultProtocol = global.location.protocol.search(/^https?:$/) === -1 ? 'http:' : ''

		var protocol = opts.protocol || defaultProtocol
		var host = opts.hostname || opts.host
		var port = opts.port
		var path = opts.path || '/'

		// Necessary for IPv6 addresses
		if (host && host.indexOf(':') !== -1)
			host = '[' + host + ']'

		// This may be a relative url. The browser should always be able to interpret it correctly.
		opts.url = (host ? (protocol + '//' + host) : '') + (port ? ':' + port : '') + path
		opts.method = (opts.method || 'GET').toUpperCase()
		opts.headers = opts.headers || {}

		// Also valid opts.auth, opts.mode

		var req = new ClientRequest(opts)
		if (cb)
			req.on('response', cb)
		return req
	}

	http.get = function get (opts, cb) {
		var req = http.request(opts, cb)
		req.end()
		return req
	}

	http.ClientRequest = ClientRequest
	http.IncomingMessage = IncomingMessage

	http.Agent = function () {}
	http.Agent.defaultMaxSockets = 4

	http.STATUS_CODES = statusCodes

	http.METHODS = [
		'CHECKOUT',
		'CONNECT',
		'COPY',
		'DELETE',
		'GET',
		'HEAD',
		'LOCK',
		'M-SEARCH',
		'MERGE',
		'MKACTIVITY',
		'MKCOL',
		'MOVE',
		'NOTIFY',
		'OPTIONS',
		'PATCH',
		'POST',
		'PROPFIND',
		'PROPPATCH',
		'PURGE',
		'PUT',
		'REPORT',
		'SEARCH',
		'SUBSCRIBE',
		'TRACE',
		'UNLOCK',
		'UNSUBSCRIBE'
	]
	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }())))

/***/ }),
/* 75 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer, global, process) {var capability = __webpack_require__(76)
	var inherits = __webpack_require__(35)
	var response = __webpack_require__(77)
	var stream = __webpack_require__(78)
	var toArrayBuffer = __webpack_require__(91)

	var IncomingMessage = response.IncomingMessage
	var rStates = response.readyStates

	function decideMode (preferBinary, useFetch) {
		if (capability.fetch && useFetch) {
			return 'fetch'
		} else if (capability.mozchunkedarraybuffer) {
			return 'moz-chunked-arraybuffer'
		} else if (capability.msstream) {
			return 'ms-stream'
		} else if (capability.arraybuffer && preferBinary) {
			return 'arraybuffer'
		} else if (capability.vbArray && preferBinary) {
			return 'text:vbarray'
		} else {
			return 'text'
		}
	}

	var ClientRequest = module.exports = function (opts) {
		var self = this
		stream.Writable.call(self)

		self._opts = opts
		self._body = []
		self._headers = {}
		if (opts.auth)
			self.setHeader('Authorization', 'Basic ' + new Buffer(opts.auth).toString('base64'))
		Object.keys(opts.headers).forEach(function (name) {
			self.setHeader(name, opts.headers[name])
		})

		var preferBinary
		var useFetch = true
		if (opts.mode === 'disable-fetch' || ('requestTimeout' in opts && !capability.abortController)) {
			// If the use of XHR should be preferred. Not typically needed.
			useFetch = false
			preferBinary = true
		} else if (opts.mode === 'prefer-streaming') {
			// If streaming is a high priority but binary compatibility and
			// the accuracy of the 'content-type' header aren't
			preferBinary = false
		} else if (opts.mode === 'allow-wrong-content-type') {
			// If streaming is more important than preserving the 'content-type' header
			preferBinary = !capability.overrideMimeType
		} else if (!opts.mode || opts.mode === 'default' || opts.mode === 'prefer-fast') {
			// Use binary if text streaming may corrupt data or the content-type header, or for speed
			preferBinary = true
		} else {
			throw new Error('Invalid value for opts.mode')
		}
		self._mode = decideMode(preferBinary, useFetch)

		self.on('finish', function () {
			self._onFinish()
		})
	}

	inherits(ClientRequest, stream.Writable)

	ClientRequest.prototype.setHeader = function (name, value) {
		var self = this
		var lowerName = name.toLowerCase()
		// This check is not necessary, but it prevents warnings from browsers about setting unsafe
		// headers. To be honest I'm not entirely sure hiding these warnings is a good thing, but
		// http-browserify did it, so I will too.
		if (unsafeHeaders.indexOf(lowerName) !== -1)
			return

		self._headers[lowerName] = {
			name: name,
			value: value
		}
	}

	ClientRequest.prototype.getHeader = function (name) {
		var header = this._headers[name.toLowerCase()]
		if (header)
			return header.value
		return null
	}

	ClientRequest.prototype.removeHeader = function (name) {
		var self = this
		delete self._headers[name.toLowerCase()]
	}

	ClientRequest.prototype._onFinish = function () {
		var self = this

		if (self._destroyed)
			return
		var opts = self._opts

		var headersObj = self._headers
		var body = null
		if (opts.method !== 'GET' && opts.method !== 'HEAD') {
			if (capability.arraybuffer) {
				body = toArrayBuffer(Buffer.concat(self._body))
			} else if (capability.blobConstructor) {
				body = new global.Blob(self._body.map(function (buffer) {
					return toArrayBuffer(buffer)
				}), {
					type: (headersObj['content-type'] || {}).value || ''
				})
			} else {
				// get utf8 string
				body = Buffer.concat(self._body).toString()
			}
		}

		// create flattened list of headers
		var headersList = []
		Object.keys(headersObj).forEach(function (keyName) {
			var name = headersObj[keyName].name
			var value = headersObj[keyName].value
			if (Array.isArray(value)) {
				value.forEach(function (v) {
					headersList.push([name, v])
				})
			} else {
				headersList.push([name, value])
			}
		})

		if (self._mode === 'fetch') {
			var signal = null
			if (capability.abortController) {
				var controller = new AbortController()
				signal = controller.signal
				self._fetchAbortController = controller

				if ('requestTimeout' in opts && opts.requestTimeout !== 0) {
					global.setTimeout(function () {
						self.emit('requestTimeout')
						if (self._fetchAbortController)
							self._fetchAbortController.abort()
					}, opts.requestTimeout)
				}
			}

			global.fetch(self._opts.url, {
				method: self._opts.method,
				headers: headersList,
				body: body || undefined,
				mode: 'cors',
				credentials: opts.withCredentials ? 'include' : 'same-origin',
				signal: signal
			}).then(function (response) {
				self._fetchResponse = response
				self._connect()
			}, function (reason) {
				self.emit('error', reason)
			})
		} else {
			var xhr = self._xhr = new global.XMLHttpRequest()
			try {
				xhr.open(self._opts.method, self._opts.url, true)
			} catch (err) {
				process.nextTick(function () {
					self.emit('error', err)
				})
				return
			}

			// Can't set responseType on really old browsers
			if ('responseType' in xhr)
				xhr.responseType = self._mode.split(':')[0]

			if ('withCredentials' in xhr)
				xhr.withCredentials = !!opts.withCredentials

			if (self._mode === 'text' && 'overrideMimeType' in xhr)
				xhr.overrideMimeType('text/plain; charset=x-user-defined')

			if ('requestTimeout' in opts) {
				xhr.timeout = opts.requestTimeout
				xhr.ontimeout = function () {
					self.emit('requestTimeout')
				}
			}

			headersList.forEach(function (header) {
				xhr.setRequestHeader(header[0], header[1])
			})

			self._response = null
			xhr.onreadystatechange = function () {
				switch (xhr.readyState) {
					case rStates.LOADING:
					case rStates.DONE:
						self._onXHRProgress()
						break
				}
			}
			// Necessary for streaming in Firefox, since xhr.response is ONLY defined
			// in onprogress, not in onreadystatechange with xhr.readyState = 3
			if (self._mode === 'moz-chunked-arraybuffer') {
				xhr.onprogress = function () {
					self._onXHRProgress()
				}
			}

			xhr.onerror = function () {
				if (self._destroyed)
					return
				self.emit('error', new Error('XHR error'))
			}

			try {
				xhr.send(body)
			} catch (err) {
				process.nextTick(function () {
					self.emit('error', err)
				})
				return
			}
		}
	}

	/**
	 * Checks if xhr.status is readable and non-zero, indicating no error.
	 * Even though the spec says it should be available in readyState 3,
	 * accessing it throws an exception in IE8
	 */
	function statusValid (xhr) {
		try {
			var status = xhr.status
			return (status !== null && status !== 0)
		} catch (e) {
			return false
		}
	}

	ClientRequest.prototype._onXHRProgress = function () {
		var self = this

		if (!statusValid(self._xhr) || self._destroyed)
			return

		if (!self._response)
			self._connect()

		self._response._onXHRProgress()
	}

	ClientRequest.prototype._connect = function () {
		var self = this

		if (self._destroyed)
			return

		self._response = new IncomingMessage(self._xhr, self._fetchResponse, self._mode)
		self._response.on('error', function(err) {
			self.emit('error', err)
		})

		self.emit('response', self._response)
	}

	ClientRequest.prototype._write = function (chunk, encoding, cb) {
		var self = this

		self._body.push(chunk)
		cb()
	}

	ClientRequest.prototype.abort = ClientRequest.prototype.destroy = function () {
		var self = this
		self._destroyed = true
		if (self._response)
			self._response._destroyed = true
		if (self._xhr)
			self._xhr.abort()
		else if (self._fetchAbortController)
			self._fetchAbortController.abort()
	}

	ClientRequest.prototype.end = function (data, encoding, cb) {
		var self = this
		if (typeof data === 'function') {
			cb = data
			data = undefined
		}

		stream.Writable.prototype.end.call(self, data, encoding, cb)
	}

	ClientRequest.prototype.flushHeaders = function () {}
	ClientRequest.prototype.setTimeout = function () {}
	ClientRequest.prototype.setNoDelay = function () {}
	ClientRequest.prototype.setSocketKeepAlive = function () {}

	// Taken from http://www.w3.org/TR/XMLHttpRequest/#the-setrequestheader%28%29-method
	var unsafeHeaders = [
		'accept-charset',
		'accept-encoding',
		'access-control-request-headers',
		'access-control-request-method',
		'connection',
		'content-length',
		'cookie',
		'cookie2',
		'date',
		'dnt',
		'expect',
		'host',
		'keep-alive',
		'origin',
		'referer',
		'te',
		'trailer',
		'transfer-encoding',
		'upgrade',
		'user-agent',
		'via'
	]

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer, (function() { return this; }()), __webpack_require__(18)))

/***/ }),
/* 76 */
/***/ (function(module, exports) {

	/* WEBPACK VAR INJECTION */(function(global) {exports.fetch = isFunction(global.fetch) && isFunction(global.ReadableStream)

	exports.writableStream = isFunction(global.WritableStream)

	exports.abortController = isFunction(global.AbortController)

	exports.blobConstructor = false
	try {
		new Blob([new ArrayBuffer(1)])
		exports.blobConstructor = true
	} catch (e) {}

	// The xhr request to example.com may violate some restrictive CSP configurations,
	// so if we're running in a browser that supports `fetch`, avoid calling getXHR()
	// and assume support for certain features below.
	var xhr
	function getXHR () {
		// Cache the xhr value
		if (xhr !== undefined) return xhr

		if (global.XMLHttpRequest) {
			xhr = new global.XMLHttpRequest()
			// If XDomainRequest is available (ie only, where xhr might not work
			// cross domain), use the page location. Otherwise use example.com
			// Note: this doesn't actually make an http request.
			try {
				xhr.open('GET', global.XDomainRequest ? '/' : 'https://example.com')
			} catch(e) {
				xhr = null
			}
		} else {
			// Service workers don't have XHR
			xhr = null
		}
		return xhr
	}

	function checkTypeSupport (type) {
		var xhr = getXHR()
		if (!xhr) return false
		try {
			xhr.responseType = type
			return xhr.responseType === type
		} catch (e) {}
		return false
	}

	// For some strange reason, Safari 7.0 reports typeof global.ArrayBuffer === 'object'.
	// Safari 7.1 appears to have fixed this bug.
	var haveArrayBuffer = typeof global.ArrayBuffer !== 'undefined'
	var haveSlice = haveArrayBuffer && isFunction(global.ArrayBuffer.prototype.slice)

	// If fetch is supported, then arraybuffer will be supported too. Skip calling
	// checkTypeSupport(), since that calls getXHR().
	exports.arraybuffer = exports.fetch || (haveArrayBuffer && checkTypeSupport('arraybuffer'))

	// These next two tests unavoidably show warnings in Chrome. Since fetch will always
	// be used if it's available, just return false for these to avoid the warnings.
	exports.msstream = !exports.fetch && haveSlice && checkTypeSupport('ms-stream')
	exports.mozchunkedarraybuffer = !exports.fetch && haveArrayBuffer &&
		checkTypeSupport('moz-chunked-arraybuffer')

	// If fetch is supported, then overrideMimeType will be supported too. Skip calling
	// getXHR().
	exports.overrideMimeType = exports.fetch || (getXHR() ? isFunction(getXHR().overrideMimeType) : false)

	exports.vbArray = isFunction(global.VBArray)

	function isFunction (value) {
		return typeof value === 'function'
	}

	xhr = null // Help gc

	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }())))

/***/ }),
/* 77 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(process, Buffer, global) {var capability = __webpack_require__(76)
	var inherits = __webpack_require__(35)
	var stream = __webpack_require__(78)

	var rStates = exports.readyStates = {
		UNSENT: 0,
		OPENED: 1,
		HEADERS_RECEIVED: 2,
		LOADING: 3,
		DONE: 4
	}

	var IncomingMessage = exports.IncomingMessage = function (xhr, response, mode) {
		var self = this
		stream.Readable.call(self)

		self._mode = mode
		self.headers = {}
		self.rawHeaders = []
		self.trailers = {}
		self.rawTrailers = []

		// Fake the 'close' event, but only once 'end' fires
		self.on('end', function () {
			// The nextTick is necessary to prevent the 'request' module from causing an infinite loop
			process.nextTick(function () {
				self.emit('close')
			})
		})

		if (mode === 'fetch') {
			self._fetchResponse = response

			self.url = response.url
			self.statusCode = response.status
			self.statusMessage = response.statusText
			
			response.headers.forEach(function (header, key){
				self.headers[key.toLowerCase()] = header
				self.rawHeaders.push(key, header)
			})

			if (capability.writableStream) {
				var writable = new WritableStream({
					write: function (chunk) {
						return new Promise(function (resolve, reject) {
							if (self._destroyed) {
								return
							} else if(self.push(new Buffer(chunk))) {
								resolve()
							} else {
								self._resumeFetch = resolve
							}
						})
					},
					close: function () {
						if (!self._destroyed)
							self.push(null)
					},
					abort: function (err) {
						if (!self._destroyed)
							self.emit('error', err)
					}
				})

				try {
					response.body.pipeTo(writable)
					return
				} catch (e) {} // pipeTo method isn't defined. Can't find a better way to feature test this
			}
			// fallback for when writableStream or pipeTo aren't available
			var reader = response.body.getReader()
			function read () {
				reader.read().then(function (result) {
					if (self._destroyed)
						return
					if (result.done) {
						self.push(null)
						return
					}
					self.push(new Buffer(result.value))
					read()
				}).catch(function(err) {
					if (!self._destroyed)
						self.emit('error', err)
				})
			}
			read()
		} else {
			self._xhr = xhr
			self._pos = 0

			self.url = xhr.responseURL
			self.statusCode = xhr.status
			self.statusMessage = xhr.statusText
			var headers = xhr.getAllResponseHeaders().split(/\r?\n/)
			headers.forEach(function (header) {
				var matches = header.match(/^([^:]+):\s*(.*)/)
				if (matches) {
					var key = matches[1].toLowerCase()
					if (key === 'set-cookie') {
						if (self.headers[key] === undefined) {
							self.headers[key] = []
						}
						self.headers[key].push(matches[2])
					} else if (self.headers[key] !== undefined) {
						self.headers[key] += ', ' + matches[2]
					} else {
						self.headers[key] = matches[2]
					}
					self.rawHeaders.push(matches[1], matches[2])
				}
			})

			self._charset = 'x-user-defined'
			if (!capability.overrideMimeType) {
				var mimeType = self.rawHeaders['mime-type']
				if (mimeType) {
					var charsetMatch = mimeType.match(/;\s*charset=([^;])(;|$)/)
					if (charsetMatch) {
						self._charset = charsetMatch[1].toLowerCase()
					}
				}
				if (!self._charset)
					self._charset = 'utf-8' // best guess
			}
		}
	}

	inherits(IncomingMessage, stream.Readable)

	IncomingMessage.prototype._read = function () {
		var self = this

		var resolve = self._resumeFetch
		if (resolve) {
			self._resumeFetch = null
			resolve()
		}
	}

	IncomingMessage.prototype._onXHRProgress = function () {
		var self = this

		var xhr = self._xhr

		var response = null
		switch (self._mode) {
			case 'text:vbarray': // For IE9
				if (xhr.readyState !== rStates.DONE)
					break
				try {
					// This fails in IE8
					response = new global.VBArray(xhr.responseBody).toArray()
				} catch (e) {}
				if (response !== null) {
					self.push(new Buffer(response))
					break
				}
				// Falls through in IE8	
			case 'text':
				try { // This will fail when readyState = 3 in IE9. Switch mode and wait for readyState = 4
					response = xhr.responseText
				} catch (e) {
					self._mode = 'text:vbarray'
					break
				}
				if (response.length > self._pos) {
					var newData = response.substr(self._pos)
					if (self._charset === 'x-user-defined') {
						var buffer = new Buffer(newData.length)
						for (var i = 0; i < newData.length; i++)
							buffer[i] = newData.charCodeAt(i) & 0xff

						self.push(buffer)
					} else {
						self.push(newData, self._charset)
					}
					self._pos = response.length
				}
				break
			case 'arraybuffer':
				if (xhr.readyState !== rStates.DONE || !xhr.response)
					break
				response = xhr.response
				self.push(new Buffer(new Uint8Array(response)))
				break
			case 'moz-chunked-arraybuffer': // take whole
				response = xhr.response
				if (xhr.readyState !== rStates.LOADING || !response)
					break
				self.push(new Buffer(new Uint8Array(response)))
				break
			case 'ms-stream':
				response = xhr.response
				if (xhr.readyState !== rStates.LOADING)
					break
				var reader = new global.MSStreamReader()
				reader.onprogress = function () {
					if (reader.result.byteLength > self._pos) {
						self.push(new Buffer(new Uint8Array(reader.result.slice(self._pos))))
						self._pos = reader.result.byteLength
					}
				}
				reader.onload = function () {
					self.push(null)
				}
				// reader.onerror = ??? // TODO: this
				reader.readAsArrayBuffer(response)
				break
		}

		// The ms-stream case handles end separately in reader.onload()
		if (self._xhr.readyState === rStates.DONE && self._mode !== 'ms-stream') {
			self.push(null)
		}
	}

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(18), __webpack_require__(5).Buffer, (function() { return this; }())))

/***/ }),
/* 78 */
/***/ (function(module, exports, __webpack_require__) {

	exports = module.exports = __webpack_require__(79);
	exports.Stream = exports;
	exports.Readable = exports;
	exports.Writable = __webpack_require__(87);
	exports.Duplex = __webpack_require__(86);
	exports.Transform = __webpack_require__(89);
	exports.PassThrough = __webpack_require__(90);


/***/ }),
/* 79 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(global, process) {// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	'use strict';

	/*<replacement>*/

	var pna = __webpack_require__(38);
	/*</replacement>*/

	module.exports = Readable;

	/*<replacement>*/
	var isArray = __webpack_require__(80);
	/*</replacement>*/

	/*<replacement>*/
	var Duplex;
	/*</replacement>*/

	Readable.ReadableState = ReadableState;

	/*<replacement>*/
	var EE = __webpack_require__(34).EventEmitter;

	var EElistenerCount = function (emitter, type) {
	  return emitter.listeners(type).length;
	};
	/*</replacement>*/

	/*<replacement>*/
	var Stream = __webpack_require__(81);
	/*</replacement>*/

	/*<replacement>*/

	var Buffer = __webpack_require__(41).Buffer;
	var OurUint8Array = global.Uint8Array || function () {};
	function _uint8ArrayToBuffer(chunk) {
	  return Buffer.from(chunk);
	}
	function _isUint8Array(obj) {
	  return Buffer.isBuffer(obj) || obj instanceof OurUint8Array;
	}

	/*</replacement>*/

	/*<replacement>*/
	var util = __webpack_require__(42);
	util.inherits = __webpack_require__(35);
	/*</replacement>*/

	/*<replacement>*/
	var debugUtil = __webpack_require__(82);
	var debug = void 0;
	if (debugUtil && debugUtil.debuglog) {
	  debug = debugUtil.debuglog('stream');
	} else {
	  debug = function () {};
	}
	/*</replacement>*/

	var BufferList = __webpack_require__(83);
	var destroyImpl = __webpack_require__(85);
	var StringDecoder;

	util.inherits(Readable, Stream);

	var kProxyEvents = ['error', 'close', 'destroy', 'pause', 'resume'];

	function prependListener(emitter, event, fn) {
	  // Sadly this is not cacheable as some libraries bundle their own
	  // event emitter implementation with them.
	  if (typeof emitter.prependListener === 'function') return emitter.prependListener(event, fn);

	  // This is a hack to make sure that our error handler is attached before any
	  // userland ones.  NEVER DO THIS. This is here only because this code needs
	  // to continue to work with older versions of Node.js that do not include
	  // the prependListener() method. The goal is to eventually remove this hack.
	  if (!emitter._events || !emitter._events[event]) emitter.on(event, fn);else if (isArray(emitter._events[event])) emitter._events[event].unshift(fn);else emitter._events[event] = [fn, emitter._events[event]];
	}

	function ReadableState(options, stream) {
	  Duplex = Duplex || __webpack_require__(86);

	  options = options || {};

	  // Duplex streams are both readable and writable, but share
	  // the same options object.
	  // However, some cases require setting options to different
	  // values for the readable and the writable sides of the duplex stream.
	  // These options can be provided separately as readableXXX and writableXXX.
	  var isDuplex = stream instanceof Duplex;

	  // object stream flag. Used to make read(n) ignore n and to
	  // make all the buffer merging and length checks go away
	  this.objectMode = !!options.objectMode;

	  if (isDuplex) this.objectMode = this.objectMode || !!options.readableObjectMode;

	  // the point at which it stops calling _read() to fill the buffer
	  // Note: 0 is a valid value, means "don't call _read preemptively ever"
	  var hwm = options.highWaterMark;
	  var readableHwm = options.readableHighWaterMark;
	  var defaultHwm = this.objectMode ? 16 : 16 * 1024;

	  if (hwm || hwm === 0) this.highWaterMark = hwm;else if (isDuplex && (readableHwm || readableHwm === 0)) this.highWaterMark = readableHwm;else this.highWaterMark = defaultHwm;

	  // cast to ints.
	  this.highWaterMark = Math.floor(this.highWaterMark);

	  // A linked list is used to store data chunks instead of an array because the
	  // linked list can remove elements from the beginning faster than
	  // array.shift()
	  this.buffer = new BufferList();
	  this.length = 0;
	  this.pipes = null;
	  this.pipesCount = 0;
	  this.flowing = null;
	  this.ended = false;
	  this.endEmitted = false;
	  this.reading = false;

	  // a flag to be able to tell if the event 'readable'/'data' is emitted
	  // immediately, or on a later tick.  We set this to true at first, because
	  // any actions that shouldn't happen until "later" should generally also
	  // not happen before the first read call.
	  this.sync = true;

	  // whenever we return null, then we set a flag to say
	  // that we're awaiting a 'readable' event emission.
	  this.needReadable = false;
	  this.emittedReadable = false;
	  this.readableListening = false;
	  this.resumeScheduled = false;

	  // has it been destroyed
	  this.destroyed = false;

	  // Crypto is kind of old and crusty.  Historically, its default string
	  // encoding is 'binary' so we have to make this configurable.
	  // Everything else in the universe uses 'utf8', though.
	  this.defaultEncoding = options.defaultEncoding || 'utf8';

	  // the number of writers that are awaiting a drain event in .pipe()s
	  this.awaitDrain = 0;

	  // if true, a maybeReadMore has been scheduled
	  this.readingMore = false;

	  this.decoder = null;
	  this.encoding = null;
	  if (options.encoding) {
	    if (!StringDecoder) StringDecoder = __webpack_require__(88).StringDecoder;
	    this.decoder = new StringDecoder(options.encoding);
	    this.encoding = options.encoding;
	  }
	}

	function Readable(options) {
	  Duplex = Duplex || __webpack_require__(86);

	  if (!(this instanceof Readable)) return new Readable(options);

	  this._readableState = new ReadableState(options, this);

	  // legacy
	  this.readable = true;

	  if (options) {
	    if (typeof options.read === 'function') this._read = options.read;

	    if (typeof options.destroy === 'function') this._destroy = options.destroy;
	  }

	  Stream.call(this);
	}

	Object.defineProperty(Readable.prototype, 'destroyed', {
	  get: function () {
	    if (this._readableState === undefined) {
	      return false;
	    }
	    return this._readableState.destroyed;
	  },
	  set: function (value) {
	    // we ignore the value if the stream
	    // has not been initialized yet
	    if (!this._readableState) {
	      return;
	    }

	    // backward compatibility, the user is explicitly
	    // managing destroyed
	    this._readableState.destroyed = value;
	  }
	});

	Readable.prototype.destroy = destroyImpl.destroy;
	Readable.prototype._undestroy = destroyImpl.undestroy;
	Readable.prototype._destroy = function (err, cb) {
	  this.push(null);
	  cb(err);
	};

	// Manually shove something into the read() buffer.
	// This returns true if the highWaterMark has not been hit yet,
	// similar to how Writable.write() returns true if you should
	// write() some more.
	Readable.prototype.push = function (chunk, encoding) {
	  var state = this._readableState;
	  var skipChunkCheck;

	  if (!state.objectMode) {
	    if (typeof chunk === 'string') {
	      encoding = encoding || state.defaultEncoding;
	      if (encoding !== state.encoding) {
	        chunk = Buffer.from(chunk, encoding);
	        encoding = '';
	      }
	      skipChunkCheck = true;
	    }
	  } else {
	    skipChunkCheck = true;
	  }

	  return readableAddChunk(this, chunk, encoding, false, skipChunkCheck);
	};

	// Unshift should *always* be something directly out of read()
	Readable.prototype.unshift = function (chunk) {
	  return readableAddChunk(this, chunk, null, true, false);
	};

	function readableAddChunk(stream, chunk, encoding, addToFront, skipChunkCheck) {
	  var state = stream._readableState;
	  if (chunk === null) {
	    state.reading = false;
	    onEofChunk(stream, state);
	  } else {
	    var er;
	    if (!skipChunkCheck) er = chunkInvalid(state, chunk);
	    if (er) {
	      stream.emit('error', er);
	    } else if (state.objectMode || chunk && chunk.length > 0) {
	      if (typeof chunk !== 'string' && !state.objectMode && Object.getPrototypeOf(chunk) !== Buffer.prototype) {
	        chunk = _uint8ArrayToBuffer(chunk);
	      }

	      if (addToFront) {
	        if (state.endEmitted) stream.emit('error', new Error('stream.unshift() after end event'));else addChunk(stream, state, chunk, true);
	      } else if (state.ended) {
	        stream.emit('error', new Error('stream.push() after EOF'));
	      } else {
	        state.reading = false;
	        if (state.decoder && !encoding) {
	          chunk = state.decoder.write(chunk);
	          if (state.objectMode || chunk.length !== 0) addChunk(stream, state, chunk, false);else maybeReadMore(stream, state);
	        } else {
	          addChunk(stream, state, chunk, false);
	        }
	      }
	    } else if (!addToFront) {
	      state.reading = false;
	    }
	  }

	  return needMoreData(state);
	}

	function addChunk(stream, state, chunk, addToFront) {
	  if (state.flowing && state.length === 0 && !state.sync) {
	    stream.emit('data', chunk);
	    stream.read(0);
	  } else {
	    // update the buffer info.
	    state.length += state.objectMode ? 1 : chunk.length;
	    if (addToFront) state.buffer.unshift(chunk);else state.buffer.push(chunk);

	    if (state.needReadable) emitReadable(stream);
	  }
	  maybeReadMore(stream, state);
	}

	function chunkInvalid(state, chunk) {
	  var er;
	  if (!_isUint8Array(chunk) && typeof chunk !== 'string' && chunk !== undefined && !state.objectMode) {
	    er = new TypeError('Invalid non-string/buffer chunk');
	  }
	  return er;
	}

	// if it's past the high water mark, we can push in some more.
	// Also, if we have no data yet, we can stand some
	// more bytes.  This is to work around cases where hwm=0,
	// such as the repl.  Also, if the push() triggered a
	// readable event, and the user called read(largeNumber) such that
	// needReadable was set, then we ought to push more, so that another
	// 'readable' event will be triggered.
	function needMoreData(state) {
	  return !state.ended && (state.needReadable || state.length < state.highWaterMark || state.length === 0);
	}

	Readable.prototype.isPaused = function () {
	  return this._readableState.flowing === false;
	};

	// backwards compatibility.
	Readable.prototype.setEncoding = function (enc) {
	  if (!StringDecoder) StringDecoder = __webpack_require__(88).StringDecoder;
	  this._readableState.decoder = new StringDecoder(enc);
	  this._readableState.encoding = enc;
	  return this;
	};

	// Don't raise the hwm > 8MB
	var MAX_HWM = 0x800000;
	function computeNewHighWaterMark(n) {
	  if (n >= MAX_HWM) {
	    n = MAX_HWM;
	  } else {
	    // Get the next highest power of 2 to prevent increasing hwm excessively in
	    // tiny amounts
	    n--;
	    n |= n >>> 1;
	    n |= n >>> 2;
	    n |= n >>> 4;
	    n |= n >>> 8;
	    n |= n >>> 16;
	    n++;
	  }
	  return n;
	}

	// This function is designed to be inlinable, so please take care when making
	// changes to the function body.
	function howMuchToRead(n, state) {
	  if (n <= 0 || state.length === 0 && state.ended) return 0;
	  if (state.objectMode) return 1;
	  if (n !== n) {
	    // Only flow one buffer at a time
	    if (state.flowing && state.length) return state.buffer.head.data.length;else return state.length;
	  }
	  // If we're asking for more than the current hwm, then raise the hwm.
	  if (n > state.highWaterMark) state.highWaterMark = computeNewHighWaterMark(n);
	  if (n <= state.length) return n;
	  // Don't have enough
	  if (!state.ended) {
	    state.needReadable = true;
	    return 0;
	  }
	  return state.length;
	}

	// you can override either this method, or the async _read(n) below.
	Readable.prototype.read = function (n) {
	  debug('read', n);
	  n = parseInt(n, 10);
	  var state = this._readableState;
	  var nOrig = n;

	  if (n !== 0) state.emittedReadable = false;

	  // if we're doing read(0) to trigger a readable event, but we
	  // already have a bunch of data in the buffer, then just trigger
	  // the 'readable' event and move on.
	  if (n === 0 && state.needReadable && (state.length >= state.highWaterMark || state.ended)) {
	    debug('read: emitReadable', state.length, state.ended);
	    if (state.length === 0 && state.ended) endReadable(this);else emitReadable(this);
	    return null;
	  }

	  n = howMuchToRead(n, state);

	  // if we've ended, and we're now clear, then finish it up.
	  if (n === 0 && state.ended) {
	    if (state.length === 0) endReadable(this);
	    return null;
	  }

	  // All the actual chunk generation logic needs to be
	  // *below* the call to _read.  The reason is that in certain
	  // synthetic stream cases, such as passthrough streams, _read
	  // may be a completely synchronous operation which may change
	  // the state of the read buffer, providing enough data when
	  // before there was *not* enough.
	  //
	  // So, the steps are:
	  // 1. Figure out what the state of things will be after we do
	  // a read from the buffer.
	  //
	  // 2. If that resulting state will trigger a _read, then call _read.
	  // Note that this may be asynchronous, or synchronous.  Yes, it is
	  // deeply ugly to write APIs this way, but that still doesn't mean
	  // that the Readable class should behave improperly, as streams are
	  // designed to be sync/async agnostic.
	  // Take note if the _read call is sync or async (ie, if the read call
	  // has returned yet), so that we know whether or not it's safe to emit
	  // 'readable' etc.
	  //
	  // 3. Actually pull the requested chunks out of the buffer and return.

	  // if we need a readable event, then we need to do some reading.
	  var doRead = state.needReadable;
	  debug('need readable', doRead);

	  // if we currently have less than the highWaterMark, then also read some
	  if (state.length === 0 || state.length - n < state.highWaterMark) {
	    doRead = true;
	    debug('length less than watermark', doRead);
	  }

	  // however, if we've ended, then there's no point, and if we're already
	  // reading, then it's unnecessary.
	  if (state.ended || state.reading) {
	    doRead = false;
	    debug('reading or ended', doRead);
	  } else if (doRead) {
	    debug('do read');
	    state.reading = true;
	    state.sync = true;
	    // if the length is currently zero, then we *need* a readable event.
	    if (state.length === 0) state.needReadable = true;
	    // call internal read method
	    this._read(state.highWaterMark);
	    state.sync = false;
	    // If _read pushed data synchronously, then `reading` will be false,
	    // and we need to re-evaluate how much data we can return to the user.
	    if (!state.reading) n = howMuchToRead(nOrig, state);
	  }

	  var ret;
	  if (n > 0) ret = fromList(n, state);else ret = null;

	  if (ret === null) {
	    state.needReadable = true;
	    n = 0;
	  } else {
	    state.length -= n;
	  }

	  if (state.length === 0) {
	    // If we have nothing in the buffer, then we want to know
	    // as soon as we *do* get something into the buffer.
	    if (!state.ended) state.needReadable = true;

	    // If we tried to read() past the EOF, then emit end on the next tick.
	    if (nOrig !== n && state.ended) endReadable(this);
	  }

	  if (ret !== null) this.emit('data', ret);

	  return ret;
	};

	function onEofChunk(stream, state) {
	  if (state.ended) return;
	  if (state.decoder) {
	    var chunk = state.decoder.end();
	    if (chunk && chunk.length) {
	      state.buffer.push(chunk);
	      state.length += state.objectMode ? 1 : chunk.length;
	    }
	  }
	  state.ended = true;

	  // emit 'readable' now to make sure it gets picked up.
	  emitReadable(stream);
	}

	// Don't emit readable right away in sync mode, because this can trigger
	// another read() call => stack overflow.  This way, it might trigger
	// a nextTick recursion warning, but that's not so bad.
	function emitReadable(stream) {
	  var state = stream._readableState;
	  state.needReadable = false;
	  if (!state.emittedReadable) {
	    debug('emitReadable', state.flowing);
	    state.emittedReadable = true;
	    if (state.sync) pna.nextTick(emitReadable_, stream);else emitReadable_(stream);
	  }
	}

	function emitReadable_(stream) {
	  debug('emit readable');
	  stream.emit('readable');
	  flow(stream);
	}

	// at this point, the user has presumably seen the 'readable' event,
	// and called read() to consume some data.  that may have triggered
	// in turn another _read(n) call, in which case reading = true if
	// it's in progress.
	// However, if we're not ended, or reading, and the length < hwm,
	// then go ahead and try to read some more preemptively.
	function maybeReadMore(stream, state) {
	  if (!state.readingMore) {
	    state.readingMore = true;
	    pna.nextTick(maybeReadMore_, stream, state);
	  }
	}

	function maybeReadMore_(stream, state) {
	  var len = state.length;
	  while (!state.reading && !state.flowing && !state.ended && state.length < state.highWaterMark) {
	    debug('maybeReadMore read 0');
	    stream.read(0);
	    if (len === state.length)
	      // didn't get any data, stop spinning.
	      break;else len = state.length;
	  }
	  state.readingMore = false;
	}

	// abstract method.  to be overridden in specific implementation classes.
	// call cb(er, data) where data is <= n in length.
	// for virtual (non-string, non-buffer) streams, "length" is somewhat
	// arbitrary, and perhaps not very meaningful.
	Readable.prototype._read = function (n) {
	  this.emit('error', new Error('_read() is not implemented'));
	};

	Readable.prototype.pipe = function (dest, pipeOpts) {
	  var src = this;
	  var state = this._readableState;

	  switch (state.pipesCount) {
	    case 0:
	      state.pipes = dest;
	      break;
	    case 1:
	      state.pipes = [state.pipes, dest];
	      break;
	    default:
	      state.pipes.push(dest);
	      break;
	  }
	  state.pipesCount += 1;
	  debug('pipe count=%d opts=%j', state.pipesCount, pipeOpts);

	  var doEnd = (!pipeOpts || pipeOpts.end !== false) && dest !== process.stdout && dest !== process.stderr;

	  var endFn = doEnd ? onend : unpipe;
	  if (state.endEmitted) pna.nextTick(endFn);else src.once('end', endFn);

	  dest.on('unpipe', onunpipe);
	  function onunpipe(readable, unpipeInfo) {
	    debug('onunpipe');
	    if (readable === src) {
	      if (unpipeInfo && unpipeInfo.hasUnpiped === false) {
	        unpipeInfo.hasUnpiped = true;
	        cleanup();
	      }
	    }
	  }

	  function onend() {
	    debug('onend');
	    dest.end();
	  }

	  // when the dest drains, it reduces the awaitDrain counter
	  // on the source.  This would be more elegant with a .once()
	  // handler in flow(), but adding and removing repeatedly is
	  // too slow.
	  var ondrain = pipeOnDrain(src);
	  dest.on('drain', ondrain);

	  var cleanedUp = false;
	  function cleanup() {
	    debug('cleanup');
	    // cleanup event handlers once the pipe is broken
	    dest.removeListener('close', onclose);
	    dest.removeListener('finish', onfinish);
	    dest.removeListener('drain', ondrain);
	    dest.removeListener('error', onerror);
	    dest.removeListener('unpipe', onunpipe);
	    src.removeListener('end', onend);
	    src.removeListener('end', unpipe);
	    src.removeListener('data', ondata);

	    cleanedUp = true;

	    // if the reader is waiting for a drain event from this
	    // specific writer, then it would cause it to never start
	    // flowing again.
	    // So, if this is awaiting a drain, then we just call it now.
	    // If we don't know, then assume that we are waiting for one.
	    if (state.awaitDrain && (!dest._writableState || dest._writableState.needDrain)) ondrain();
	  }

	  // If the user pushes more data while we're writing to dest then we'll end up
	  // in ondata again. However, we only want to increase awaitDrain once because
	  // dest will only emit one 'drain' event for the multiple writes.
	  // => Introduce a guard on increasing awaitDrain.
	  var increasedAwaitDrain = false;
	  src.on('data', ondata);
	  function ondata(chunk) {
	    debug('ondata');
	    increasedAwaitDrain = false;
	    var ret = dest.write(chunk);
	    if (false === ret && !increasedAwaitDrain) {
	      // If the user unpiped during `dest.write()`, it is possible
	      // to get stuck in a permanently paused state if that write
	      // also returned false.
	      // => Check whether `dest` is still a piping destination.
	      if ((state.pipesCount === 1 && state.pipes === dest || state.pipesCount > 1 && indexOf(state.pipes, dest) !== -1) && !cleanedUp) {
	        debug('false write response, pause', src._readableState.awaitDrain);
	        src._readableState.awaitDrain++;
	        increasedAwaitDrain = true;
	      }
	      src.pause();
	    }
	  }

	  // if the dest has an error, then stop piping into it.
	  // however, don't suppress the throwing behavior for this.
	  function onerror(er) {
	    debug('onerror', er);
	    unpipe();
	    dest.removeListener('error', onerror);
	    if (EElistenerCount(dest, 'error') === 0) dest.emit('error', er);
	  }

	  // Make sure our error handler is attached before userland ones.
	  prependListener(dest, 'error', onerror);

	  // Both close and finish should trigger unpipe, but only once.
	  function onclose() {
	    dest.removeListener('finish', onfinish);
	    unpipe();
	  }
	  dest.once('close', onclose);
	  function onfinish() {
	    debug('onfinish');
	    dest.removeListener('close', onclose);
	    unpipe();
	  }
	  dest.once('finish', onfinish);

	  function unpipe() {
	    debug('unpipe');
	    src.unpipe(dest);
	  }

	  // tell the dest that it's being piped to
	  dest.emit('pipe', src);

	  // start the flow if it hasn't been started already.
	  if (!state.flowing) {
	    debug('pipe resume');
	    src.resume();
	  }

	  return dest;
	};

	function pipeOnDrain(src) {
	  return function () {
	    var state = src._readableState;
	    debug('pipeOnDrain', state.awaitDrain);
	    if (state.awaitDrain) state.awaitDrain--;
	    if (state.awaitDrain === 0 && EElistenerCount(src, 'data')) {
	      state.flowing = true;
	      flow(src);
	    }
	  };
	}

	Readable.prototype.unpipe = function (dest) {
	  var state = this._readableState;
	  var unpipeInfo = { hasUnpiped: false };

	  // if we're not piping anywhere, then do nothing.
	  if (state.pipesCount === 0) return this;

	  // just one destination.  most common case.
	  if (state.pipesCount === 1) {
	    // passed in one, but it's not the right one.
	    if (dest && dest !== state.pipes) return this;

	    if (!dest) dest = state.pipes;

	    // got a match.
	    state.pipes = null;
	    state.pipesCount = 0;
	    state.flowing = false;
	    if (dest) dest.emit('unpipe', this, unpipeInfo);
	    return this;
	  }

	  // slow case. multiple pipe destinations.

	  if (!dest) {
	    // remove all.
	    var dests = state.pipes;
	    var len = state.pipesCount;
	    state.pipes = null;
	    state.pipesCount = 0;
	    state.flowing = false;

	    for (var i = 0; i < len; i++) {
	      dests[i].emit('unpipe', this, unpipeInfo);
	    }return this;
	  }

	  // try to find the right one.
	  var index = indexOf(state.pipes, dest);
	  if (index === -1) return this;

	  state.pipes.splice(index, 1);
	  state.pipesCount -= 1;
	  if (state.pipesCount === 1) state.pipes = state.pipes[0];

	  dest.emit('unpipe', this, unpipeInfo);

	  return this;
	};

	// set up data events if they are asked for
	// Ensure readable listeners eventually get something
	Readable.prototype.on = function (ev, fn) {
	  var res = Stream.prototype.on.call(this, ev, fn);

	  if (ev === 'data') {
	    // Start flowing on next tick if stream isn't explicitly paused
	    if (this._readableState.flowing !== false) this.resume();
	  } else if (ev === 'readable') {
	    var state = this._readableState;
	    if (!state.endEmitted && !state.readableListening) {
	      state.readableListening = state.needReadable = true;
	      state.emittedReadable = false;
	      if (!state.reading) {
	        pna.nextTick(nReadingNextTick, this);
	      } else if (state.length) {
	        emitReadable(this);
	      }
	    }
	  }

	  return res;
	};
	Readable.prototype.addListener = Readable.prototype.on;

	function nReadingNextTick(self) {
	  debug('readable nexttick read 0');
	  self.read(0);
	}

	// pause() and resume() are remnants of the legacy readable stream API
	// If the user uses them, then switch into old mode.
	Readable.prototype.resume = function () {
	  var state = this._readableState;
	  if (!state.flowing) {
	    debug('resume');
	    state.flowing = true;
	    resume(this, state);
	  }
	  return this;
	};

	function resume(stream, state) {
	  if (!state.resumeScheduled) {
	    state.resumeScheduled = true;
	    pna.nextTick(resume_, stream, state);
	  }
	}

	function resume_(stream, state) {
	  if (!state.reading) {
	    debug('resume read 0');
	    stream.read(0);
	  }

	  state.resumeScheduled = false;
	  state.awaitDrain = 0;
	  stream.emit('resume');
	  flow(stream);
	  if (state.flowing && !state.reading) stream.read(0);
	}

	Readable.prototype.pause = function () {
	  debug('call pause flowing=%j', this._readableState.flowing);
	  if (false !== this._readableState.flowing) {
	    debug('pause');
	    this._readableState.flowing = false;
	    this.emit('pause');
	  }
	  return this;
	};

	function flow(stream) {
	  var state = stream._readableState;
	  debug('flow', state.flowing);
	  while (state.flowing && stream.read() !== null) {}
	}

	// wrap an old-style stream as the async data source.
	// This is *not* part of the readable stream interface.
	// It is an ugly unfortunate mess of history.
	Readable.prototype.wrap = function (stream) {
	  var _this = this;

	  var state = this._readableState;
	  var paused = false;

	  stream.on('end', function () {
	    debug('wrapped end');
	    if (state.decoder && !state.ended) {
	      var chunk = state.decoder.end();
	      if (chunk && chunk.length) _this.push(chunk);
	    }

	    _this.push(null);
	  });

	  stream.on('data', function (chunk) {
	    debug('wrapped data');
	    if (state.decoder) chunk = state.decoder.write(chunk);

	    // don't skip over falsy values in objectMode
	    if (state.objectMode && (chunk === null || chunk === undefined)) return;else if (!state.objectMode && (!chunk || !chunk.length)) return;

	    var ret = _this.push(chunk);
	    if (!ret) {
	      paused = true;
	      stream.pause();
	    }
	  });

	  // proxy all the other methods.
	  // important when wrapping filters and duplexes.
	  for (var i in stream) {
	    if (this[i] === undefined && typeof stream[i] === 'function') {
	      this[i] = function (method) {
	        return function () {
	          return stream[method].apply(stream, arguments);
	        };
	      }(i);
	    }
	  }

	  // proxy certain important events.
	  for (var n = 0; n < kProxyEvents.length; n++) {
	    stream.on(kProxyEvents[n], this.emit.bind(this, kProxyEvents[n]));
	  }

	  // when we try to consume some more bytes, simply unpause the
	  // underlying stream.
	  this._read = function (n) {
	    debug('wrapped _read', n);
	    if (paused) {
	      paused = false;
	      stream.resume();
	    }
	  };

	  return this;
	};

	// exposed for testing purposes only.
	Readable._fromList = fromList;

	// Pluck off n bytes from an array of buffers.
	// Length is the combined lengths of all the buffers in the list.
	// This function is designed to be inlinable, so please take care when making
	// changes to the function body.
	function fromList(n, state) {
	  // nothing buffered
	  if (state.length === 0) return null;

	  var ret;
	  if (state.objectMode) ret = state.buffer.shift();else if (!n || n >= state.length) {
	    // read it all, truncate the list
	    if (state.decoder) ret = state.buffer.join('');else if (state.buffer.length === 1) ret = state.buffer.head.data;else ret = state.buffer.concat(state.length);
	    state.buffer.clear();
	  } else {
	    // read part of list
	    ret = fromListPartial(n, state.buffer, state.decoder);
	  }

	  return ret;
	}

	// Extracts only enough buffered data to satisfy the amount requested.
	// This function is designed to be inlinable, so please take care when making
	// changes to the function body.
	function fromListPartial(n, list, hasStrings) {
	  var ret;
	  if (n < list.head.data.length) {
	    // slice is the same for buffers and strings
	    ret = list.head.data.slice(0, n);
	    list.head.data = list.head.data.slice(n);
	  } else if (n === list.head.data.length) {
	    // first chunk is a perfect match
	    ret = list.shift();
	  } else {
	    // result spans more than one buffer
	    ret = hasStrings ? copyFromBufferString(n, list) : copyFromBuffer(n, list);
	  }
	  return ret;
	}

	// Copies a specified amount of characters from the list of buffered data
	// chunks.
	// This function is designed to be inlinable, so please take care when making
	// changes to the function body.
	function copyFromBufferString(n, list) {
	  var p = list.head;
	  var c = 1;
	  var ret = p.data;
	  n -= ret.length;
	  while (p = p.next) {
	    var str = p.data;
	    var nb = n > str.length ? str.length : n;
	    if (nb === str.length) ret += str;else ret += str.slice(0, n);
	    n -= nb;
	    if (n === 0) {
	      if (nb === str.length) {
	        ++c;
	        if (p.next) list.head = p.next;else list.head = list.tail = null;
	      } else {
	        list.head = p;
	        p.data = str.slice(nb);
	      }
	      break;
	    }
	    ++c;
	  }
	  list.length -= c;
	  return ret;
	}

	// Copies a specified amount of bytes from the list of buffered data chunks.
	// This function is designed to be inlinable, so please take care when making
	// changes to the function body.
	function copyFromBuffer(n, list) {
	  var ret = Buffer.allocUnsafe(n);
	  var p = list.head;
	  var c = 1;
	  p.data.copy(ret);
	  n -= p.data.length;
	  while (p = p.next) {
	    var buf = p.data;
	    var nb = n > buf.length ? buf.length : n;
	    buf.copy(ret, ret.length - n, 0, nb);
	    n -= nb;
	    if (n === 0) {
	      if (nb === buf.length) {
	        ++c;
	        if (p.next) list.head = p.next;else list.head = list.tail = null;
	      } else {
	        list.head = p;
	        p.data = buf.slice(nb);
	      }
	      break;
	    }
	    ++c;
	  }
	  list.length -= c;
	  return ret;
	}

	function endReadable(stream) {
	  var state = stream._readableState;

	  // If we get here before consuming all the bytes, then that is a
	  // bug in node.  Should never happen.
	  if (state.length > 0) throw new Error('"endReadable()" called on non-empty stream');

	  if (!state.endEmitted) {
	    state.ended = true;
	    pna.nextTick(endReadableNT, state, stream);
	  }
	}

	function endReadableNT(state, stream) {
	  // Check that we didn't get one last unshift.
	  if (!state.endEmitted && state.length === 0) {
	    state.endEmitted = true;
	    stream.readable = false;
	    stream.emit('end');
	  }
	}

	function forEach(xs, f) {
	  for (var i = 0, l = xs.length; i < l; i++) {
	    f(xs[i], i);
	  }
	}

	function indexOf(xs, x) {
	  for (var i = 0, l = xs.length; i < l; i++) {
	    if (xs[i] === x) return i;
	  }
	  return -1;
	}
	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }()), __webpack_require__(18)))

/***/ }),
/* 80 */
/***/ (function(module, exports) {

	var toString = {}.toString;

	module.exports = Array.isArray || function (arr) {
	  return toString.call(arr) == '[object Array]';
	};


/***/ }),
/* 81 */
/***/ (function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__(34).EventEmitter;


/***/ }),
/* 82 */
/***/ (function(module, exports) {

	/* (ignored) */

/***/ }),
/* 83 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

	var Buffer = __webpack_require__(41).Buffer;
	var util = __webpack_require__(84);

	function copyBuffer(src, target, offset) {
	  src.copy(target, offset);
	}

	module.exports = function () {
	  function BufferList() {
	    _classCallCheck(this, BufferList);

	    this.head = null;
	    this.tail = null;
	    this.length = 0;
	  }

	  BufferList.prototype.push = function push(v) {
	    var entry = { data: v, next: null };
	    if (this.length > 0) this.tail.next = entry;else this.head = entry;
	    this.tail = entry;
	    ++this.length;
	  };

	  BufferList.prototype.unshift = function unshift(v) {
	    var entry = { data: v, next: this.head };
	    if (this.length === 0) this.tail = entry;
	    this.head = entry;
	    ++this.length;
	  };

	  BufferList.prototype.shift = function shift() {
	    if (this.length === 0) return;
	    var ret = this.head.data;
	    if (this.length === 1) this.head = this.tail = null;else this.head = this.head.next;
	    --this.length;
	    return ret;
	  };

	  BufferList.prototype.clear = function clear() {
	    this.head = this.tail = null;
	    this.length = 0;
	  };

	  BufferList.prototype.join = function join(s) {
	    if (this.length === 0) return '';
	    var p = this.head;
	    var ret = '' + p.data;
	    while (p = p.next) {
	      ret += s + p.data;
	    }return ret;
	  };

	  BufferList.prototype.concat = function concat(n) {
	    if (this.length === 0) return Buffer.alloc(0);
	    if (this.length === 1) return this.head.data;
	    var ret = Buffer.allocUnsafe(n >>> 0);
	    var p = this.head;
	    var i = 0;
	    while (p) {
	      copyBuffer(p.data, ret, i);
	      i += p.data.length;
	      p = p.next;
	    }
	    return ret;
	  };

	  return BufferList;
	}();

	if (util && util.inspect && util.inspect.custom) {
	  module.exports.prototype[util.inspect.custom] = function () {
	    var obj = util.inspect({ length: this.length });
	    return this.constructor.name + ' ' + obj;
	  };
	}

/***/ }),
/* 84 */
/***/ (function(module, exports) {

	/* (ignored) */

/***/ }),
/* 85 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	/*<replacement>*/

	var pna = __webpack_require__(38);
	/*</replacement>*/

	// undocumented cb() API, needed for core, not for public API
	function destroy(err, cb) {
	  var _this = this;

	  var readableDestroyed = this._readableState && this._readableState.destroyed;
	  var writableDestroyed = this._writableState && this._writableState.destroyed;

	  if (readableDestroyed || writableDestroyed) {
	    if (cb) {
	      cb(err);
	    } else if (err && (!this._writableState || !this._writableState.errorEmitted)) {
	      pna.nextTick(emitErrorNT, this, err);
	    }
	    return this;
	  }

	  // we set destroyed to true before firing error callbacks in order
	  // to make it re-entrance safe in case destroy() is called within callbacks

	  if (this._readableState) {
	    this._readableState.destroyed = true;
	  }

	  // if this is a duplex stream mark the writable part as destroyed as well
	  if (this._writableState) {
	    this._writableState.destroyed = true;
	  }

	  this._destroy(err || null, function (err) {
	    if (!cb && err) {
	      pna.nextTick(emitErrorNT, _this, err);
	      if (_this._writableState) {
	        _this._writableState.errorEmitted = true;
	      }
	    } else if (cb) {
	      cb(err);
	    }
	  });

	  return this;
	}

	function undestroy() {
	  if (this._readableState) {
	    this._readableState.destroyed = false;
	    this._readableState.reading = false;
	    this._readableState.ended = false;
	    this._readableState.endEmitted = false;
	  }

	  if (this._writableState) {
	    this._writableState.destroyed = false;
	    this._writableState.ended = false;
	    this._writableState.ending = false;
	    this._writableState.finished = false;
	    this._writableState.errorEmitted = false;
	  }
	}

	function emitErrorNT(self, err) {
	  self.emit('error', err);
	}

	module.exports = {
	  destroy: destroy,
	  undestroy: undestroy
	};

/***/ }),
/* 86 */
/***/ (function(module, exports, __webpack_require__) {

	// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	// a duplex stream is just a stream that is both readable and writable.
	// Since JS doesn't have multiple prototypal inheritance, this class
	// prototypally inherits from Readable, and then parasitically from
	// Writable.

	'use strict';

	/*<replacement>*/

	var pna = __webpack_require__(38);
	/*</replacement>*/

	/*<replacement>*/
	var objectKeys = Object.keys || function (obj) {
	  var keys = [];
	  for (var key in obj) {
	    keys.push(key);
	  }return keys;
	};
	/*</replacement>*/

	module.exports = Duplex;

	/*<replacement>*/
	var util = __webpack_require__(42);
	util.inherits = __webpack_require__(35);
	/*</replacement>*/

	var Readable = __webpack_require__(79);
	var Writable = __webpack_require__(87);

	util.inherits(Duplex, Readable);

	var keys = objectKeys(Writable.prototype);
	for (var v = 0; v < keys.length; v++) {
	  var method = keys[v];
	  if (!Duplex.prototype[method]) Duplex.prototype[method] = Writable.prototype[method];
	}

	function Duplex(options) {
	  if (!(this instanceof Duplex)) return new Duplex(options);

	  Readable.call(this, options);
	  Writable.call(this, options);

	  if (options && options.readable === false) this.readable = false;

	  if (options && options.writable === false) this.writable = false;

	  this.allowHalfOpen = true;
	  if (options && options.allowHalfOpen === false) this.allowHalfOpen = false;

	  this.once('end', onend);
	}

	// the no-half-open enforcer
	function onend() {
	  // if we allow half-open state, or if the writable side ended,
	  // then we're ok.
	  if (this.allowHalfOpen || this._writableState.ended) return;

	  // no more data can be written.
	  // But allow more writes to happen in this tick.
	  pna.nextTick(onEndNT, this);
	}

	function onEndNT(self) {
	  self.end();
	}

	Object.defineProperty(Duplex.prototype, 'destroyed', {
	  get: function () {
	    if (this._readableState === undefined || this._writableState === undefined) {
	      return false;
	    }
	    return this._readableState.destroyed && this._writableState.destroyed;
	  },
	  set: function (value) {
	    // we ignore the value if the stream
	    // has not been initialized yet
	    if (this._readableState === undefined || this._writableState === undefined) {
	      return;
	    }

	    // backward compatibility, the user is explicitly
	    // managing destroyed
	    this._readableState.destroyed = value;
	    this._writableState.destroyed = value;
	  }
	});

	Duplex.prototype._destroy = function (err, cb) {
	  this.push(null);
	  this.end();

	  pna.nextTick(cb, err);
	};

	function forEach(xs, f) {
	  for (var i = 0, l = xs.length; i < l; i++) {
	    f(xs[i], i);
	  }
	}

/***/ }),
/* 87 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(process, setImmediate, global) {// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	// A bit simpler than readable streams.
	// Implement an async ._write(chunk, encoding, cb), and it'll handle all
	// the drain event emission and buffering.

	'use strict';

	/*<replacement>*/

	var pna = __webpack_require__(38);
	/*</replacement>*/

	module.exports = Writable;

	/* <replacement> */
	function WriteReq(chunk, encoding, cb) {
	  this.chunk = chunk;
	  this.encoding = encoding;
	  this.callback = cb;
	  this.next = null;
	}

	// It seems a linked list but it is not
	// there will be only 2 of these for each stream
	function CorkedRequest(state) {
	  var _this = this;

	  this.next = null;
	  this.entry = null;
	  this.finish = function () {
	    onCorkedFinish(_this, state);
	  };
	}
	/* </replacement> */

	/*<replacement>*/
	var asyncWrite = !process.browser && ['v0.10', 'v0.9.'].indexOf(process.version.slice(0, 5)) > -1 ? setImmediate : pna.nextTick;
	/*</replacement>*/

	/*<replacement>*/
	var Duplex;
	/*</replacement>*/

	Writable.WritableState = WritableState;

	/*<replacement>*/
	var util = __webpack_require__(42);
	util.inherits = __webpack_require__(35);
	/*</replacement>*/

	/*<replacement>*/
	var internalUtil = {
	  deprecate: __webpack_require__(51)
	};
	/*</replacement>*/

	/*<replacement>*/
	var Stream = __webpack_require__(81);
	/*</replacement>*/

	/*<replacement>*/

	var Buffer = __webpack_require__(41).Buffer;
	var OurUint8Array = global.Uint8Array || function () {};
	function _uint8ArrayToBuffer(chunk) {
	  return Buffer.from(chunk);
	}
	function _isUint8Array(obj) {
	  return Buffer.isBuffer(obj) || obj instanceof OurUint8Array;
	}

	/*</replacement>*/

	var destroyImpl = __webpack_require__(85);

	util.inherits(Writable, Stream);

	function nop() {}

	function WritableState(options, stream) {
	  Duplex = Duplex || __webpack_require__(86);

	  options = options || {};

	  // Duplex streams are both readable and writable, but share
	  // the same options object.
	  // However, some cases require setting options to different
	  // values for the readable and the writable sides of the duplex stream.
	  // These options can be provided separately as readableXXX and writableXXX.
	  var isDuplex = stream instanceof Duplex;

	  // object stream flag to indicate whether or not this stream
	  // contains buffers or objects.
	  this.objectMode = !!options.objectMode;

	  if (isDuplex) this.objectMode = this.objectMode || !!options.writableObjectMode;

	  // the point at which write() starts returning false
	  // Note: 0 is a valid value, means that we always return false if
	  // the entire buffer is not flushed immediately on write()
	  var hwm = options.highWaterMark;
	  var writableHwm = options.writableHighWaterMark;
	  var defaultHwm = this.objectMode ? 16 : 16 * 1024;

	  if (hwm || hwm === 0) this.highWaterMark = hwm;else if (isDuplex && (writableHwm || writableHwm === 0)) this.highWaterMark = writableHwm;else this.highWaterMark = defaultHwm;

	  // cast to ints.
	  this.highWaterMark = Math.floor(this.highWaterMark);

	  // if _final has been called
	  this.finalCalled = false;

	  // drain event flag.
	  this.needDrain = false;
	  // at the start of calling end()
	  this.ending = false;
	  // when end() has been called, and returned
	  this.ended = false;
	  // when 'finish' is emitted
	  this.finished = false;

	  // has it been destroyed
	  this.destroyed = false;

	  // should we decode strings into buffers before passing to _write?
	  // this is here so that some node-core streams can optimize string
	  // handling at a lower level.
	  var noDecode = options.decodeStrings === false;
	  this.decodeStrings = !noDecode;

	  // Crypto is kind of old and crusty.  Historically, its default string
	  // encoding is 'binary' so we have to make this configurable.
	  // Everything else in the universe uses 'utf8', though.
	  this.defaultEncoding = options.defaultEncoding || 'utf8';

	  // not an actual buffer we keep track of, but a measurement
	  // of how much we're waiting to get pushed to some underlying
	  // socket or file.
	  this.length = 0;

	  // a flag to see when we're in the middle of a write.
	  this.writing = false;

	  // when true all writes will be buffered until .uncork() call
	  this.corked = 0;

	  // a flag to be able to tell if the onwrite cb is called immediately,
	  // or on a later tick.  We set this to true at first, because any
	  // actions that shouldn't happen until "later" should generally also
	  // not happen before the first write call.
	  this.sync = true;

	  // a flag to know if we're processing previously buffered items, which
	  // may call the _write() callback in the same tick, so that we don't
	  // end up in an overlapped onwrite situation.
	  this.bufferProcessing = false;

	  // the callback that's passed to _write(chunk,cb)
	  this.onwrite = function (er) {
	    onwrite(stream, er);
	  };

	  // the callback that the user supplies to write(chunk,encoding,cb)
	  this.writecb = null;

	  // the amount that is being written when _write is called.
	  this.writelen = 0;

	  this.bufferedRequest = null;
	  this.lastBufferedRequest = null;

	  // number of pending user-supplied write callbacks
	  // this must be 0 before 'finish' can be emitted
	  this.pendingcb = 0;

	  // emit prefinish if the only thing we're waiting for is _write cbs
	  // This is relevant for synchronous Transform streams
	  this.prefinished = false;

	  // True if the error was already emitted and should not be thrown again
	  this.errorEmitted = false;

	  // count buffered requests
	  this.bufferedRequestCount = 0;

	  // allocate the first CorkedRequest, there is always
	  // one allocated and free to use, and we maintain at most two
	  this.corkedRequestsFree = new CorkedRequest(this);
	}

	WritableState.prototype.getBuffer = function getBuffer() {
	  var current = this.bufferedRequest;
	  var out = [];
	  while (current) {
	    out.push(current);
	    current = current.next;
	  }
	  return out;
	};

	(function () {
	  try {
	    Object.defineProperty(WritableState.prototype, 'buffer', {
	      get: internalUtil.deprecate(function () {
	        return this.getBuffer();
	      }, '_writableState.buffer is deprecated. Use _writableState.getBuffer ' + 'instead.', 'DEP0003')
	    });
	  } catch (_) {}
	})();

	// Test _writableState for inheritance to account for Duplex streams,
	// whose prototype chain only points to Readable.
	var realHasInstance;
	if (typeof Symbol === 'function' && Symbol.hasInstance && typeof Function.prototype[Symbol.hasInstance] === 'function') {
	  realHasInstance = Function.prototype[Symbol.hasInstance];
	  Object.defineProperty(Writable, Symbol.hasInstance, {
	    value: function (object) {
	      if (realHasInstance.call(this, object)) return true;
	      if (this !== Writable) return false;

	      return object && object._writableState instanceof WritableState;
	    }
	  });
	} else {
	  realHasInstance = function (object) {
	    return object instanceof this;
	  };
	}

	function Writable(options) {
	  Duplex = Duplex || __webpack_require__(86);

	  // Writable ctor is applied to Duplexes, too.
	  // `realHasInstance` is necessary because using plain `instanceof`
	  // would return false, as no `_writableState` property is attached.

	  // Trying to use the custom `instanceof` for Writable here will also break the
	  // Node.js LazyTransform implementation, which has a non-trivial getter for
	  // `_writableState` that would lead to infinite recursion.
	  if (!realHasInstance.call(Writable, this) && !(this instanceof Duplex)) {
	    return new Writable(options);
	  }

	  this._writableState = new WritableState(options, this);

	  // legacy.
	  this.writable = true;

	  if (options) {
	    if (typeof options.write === 'function') this._write = options.write;

	    if (typeof options.writev === 'function') this._writev = options.writev;

	    if (typeof options.destroy === 'function') this._destroy = options.destroy;

	    if (typeof options.final === 'function') this._final = options.final;
	  }

	  Stream.call(this);
	}

	// Otherwise people can pipe Writable streams, which is just wrong.
	Writable.prototype.pipe = function () {
	  this.emit('error', new Error('Cannot pipe, not readable'));
	};

	function writeAfterEnd(stream, cb) {
	  var er = new Error('write after end');
	  // TODO: defer error events consistently everywhere, not just the cb
	  stream.emit('error', er);
	  pna.nextTick(cb, er);
	}

	// Checks that a user-supplied chunk is valid, especially for the particular
	// mode the stream is in. Currently this means that `null` is never accepted
	// and undefined/non-string values are only allowed in object mode.
	function validChunk(stream, state, chunk, cb) {
	  var valid = true;
	  var er = false;

	  if (chunk === null) {
	    er = new TypeError('May not write null values to stream');
	  } else if (typeof chunk !== 'string' && chunk !== undefined && !state.objectMode) {
	    er = new TypeError('Invalid non-string/buffer chunk');
	  }
	  if (er) {
	    stream.emit('error', er);
	    pna.nextTick(cb, er);
	    valid = false;
	  }
	  return valid;
	}

	Writable.prototype.write = function (chunk, encoding, cb) {
	  var state = this._writableState;
	  var ret = false;
	  var isBuf = !state.objectMode && _isUint8Array(chunk);

	  if (isBuf && !Buffer.isBuffer(chunk)) {
	    chunk = _uint8ArrayToBuffer(chunk);
	  }

	  if (typeof encoding === 'function') {
	    cb = encoding;
	    encoding = null;
	  }

	  if (isBuf) encoding = 'buffer';else if (!encoding) encoding = state.defaultEncoding;

	  if (typeof cb !== 'function') cb = nop;

	  if (state.ended) writeAfterEnd(this, cb);else if (isBuf || validChunk(this, state, chunk, cb)) {
	    state.pendingcb++;
	    ret = writeOrBuffer(this, state, isBuf, chunk, encoding, cb);
	  }

	  return ret;
	};

	Writable.prototype.cork = function () {
	  var state = this._writableState;

	  state.corked++;
	};

	Writable.prototype.uncork = function () {
	  var state = this._writableState;

	  if (state.corked) {
	    state.corked--;

	    if (!state.writing && !state.corked && !state.finished && !state.bufferProcessing && state.bufferedRequest) clearBuffer(this, state);
	  }
	};

	Writable.prototype.setDefaultEncoding = function setDefaultEncoding(encoding) {
	  // node::ParseEncoding() requires lower case.
	  if (typeof encoding === 'string') encoding = encoding.toLowerCase();
	  if (!(['hex', 'utf8', 'utf-8', 'ascii', 'binary', 'base64', 'ucs2', 'ucs-2', 'utf16le', 'utf-16le', 'raw'].indexOf((encoding + '').toLowerCase()) > -1)) throw new TypeError('Unknown encoding: ' + encoding);
	  this._writableState.defaultEncoding = encoding;
	  return this;
	};

	function decodeChunk(state, chunk, encoding) {
	  if (!state.objectMode && state.decodeStrings !== false && typeof chunk === 'string') {
	    chunk = Buffer.from(chunk, encoding);
	  }
	  return chunk;
	}

	// if we're already writing something, then just put this
	// in the queue, and wait our turn.  Otherwise, call _write
	// If we return false, then we need a drain event, so set that flag.
	function writeOrBuffer(stream, state, isBuf, chunk, encoding, cb) {
	  if (!isBuf) {
	    var newChunk = decodeChunk(state, chunk, encoding);
	    if (chunk !== newChunk) {
	      isBuf = true;
	      encoding = 'buffer';
	      chunk = newChunk;
	    }
	  }
	  var len = state.objectMode ? 1 : chunk.length;

	  state.length += len;

	  var ret = state.length < state.highWaterMark;
	  // we must ensure that previous needDrain will not be reset to false.
	  if (!ret) state.needDrain = true;

	  if (state.writing || state.corked) {
	    var last = state.lastBufferedRequest;
	    state.lastBufferedRequest = {
	      chunk: chunk,
	      encoding: encoding,
	      isBuf: isBuf,
	      callback: cb,
	      next: null
	    };
	    if (last) {
	      last.next = state.lastBufferedRequest;
	    } else {
	      state.bufferedRequest = state.lastBufferedRequest;
	    }
	    state.bufferedRequestCount += 1;
	  } else {
	    doWrite(stream, state, false, len, chunk, encoding, cb);
	  }

	  return ret;
	}

	function doWrite(stream, state, writev, len, chunk, encoding, cb) {
	  state.writelen = len;
	  state.writecb = cb;
	  state.writing = true;
	  state.sync = true;
	  if (writev) stream._writev(chunk, state.onwrite);else stream._write(chunk, encoding, state.onwrite);
	  state.sync = false;
	}

	function onwriteError(stream, state, sync, er, cb) {
	  --state.pendingcb;

	  if (sync) {
	    // defer the callback if we are being called synchronously
	    // to avoid piling up things on the stack
	    pna.nextTick(cb, er);
	    // this can emit finish, and it will always happen
	    // after error
	    pna.nextTick(finishMaybe, stream, state);
	    stream._writableState.errorEmitted = true;
	    stream.emit('error', er);
	  } else {
	    // the caller expect this to happen before if
	    // it is async
	    cb(er);
	    stream._writableState.errorEmitted = true;
	    stream.emit('error', er);
	    // this can emit finish, but finish must
	    // always follow error
	    finishMaybe(stream, state);
	  }
	}

	function onwriteStateUpdate(state) {
	  state.writing = false;
	  state.writecb = null;
	  state.length -= state.writelen;
	  state.writelen = 0;
	}

	function onwrite(stream, er) {
	  var state = stream._writableState;
	  var sync = state.sync;
	  var cb = state.writecb;

	  onwriteStateUpdate(state);

	  if (er) onwriteError(stream, state, sync, er, cb);else {
	    // Check if we're actually ready to finish, but don't emit yet
	    var finished = needFinish(state);

	    if (!finished && !state.corked && !state.bufferProcessing && state.bufferedRequest) {
	      clearBuffer(stream, state);
	    }

	    if (sync) {
	      /*<replacement>*/
	      asyncWrite(afterWrite, stream, state, finished, cb);
	      /*</replacement>*/
	    } else {
	      afterWrite(stream, state, finished, cb);
	    }
	  }
	}

	function afterWrite(stream, state, finished, cb) {
	  if (!finished) onwriteDrain(stream, state);
	  state.pendingcb--;
	  cb();
	  finishMaybe(stream, state);
	}

	// Must force callback to be called on nextTick, so that we don't
	// emit 'drain' before the write() consumer gets the 'false' return
	// value, and has a chance to attach a 'drain' listener.
	function onwriteDrain(stream, state) {
	  if (state.length === 0 && state.needDrain) {
	    state.needDrain = false;
	    stream.emit('drain');
	  }
	}

	// if there's something in the buffer waiting, then process it
	function clearBuffer(stream, state) {
	  state.bufferProcessing = true;
	  var entry = state.bufferedRequest;

	  if (stream._writev && entry && entry.next) {
	    // Fast case, write everything using _writev()
	    var l = state.bufferedRequestCount;
	    var buffer = new Array(l);
	    var holder = state.corkedRequestsFree;
	    holder.entry = entry;

	    var count = 0;
	    var allBuffers = true;
	    while (entry) {
	      buffer[count] = entry;
	      if (!entry.isBuf) allBuffers = false;
	      entry = entry.next;
	      count += 1;
	    }
	    buffer.allBuffers = allBuffers;

	    doWrite(stream, state, true, state.length, buffer, '', holder.finish);

	    // doWrite is almost always async, defer these to save a bit of time
	    // as the hot path ends with doWrite
	    state.pendingcb++;
	    state.lastBufferedRequest = null;
	    if (holder.next) {
	      state.corkedRequestsFree = holder.next;
	      holder.next = null;
	    } else {
	      state.corkedRequestsFree = new CorkedRequest(state);
	    }
	    state.bufferedRequestCount = 0;
	  } else {
	    // Slow case, write chunks one-by-one
	    while (entry) {
	      var chunk = entry.chunk;
	      var encoding = entry.encoding;
	      var cb = entry.callback;
	      var len = state.objectMode ? 1 : chunk.length;

	      doWrite(stream, state, false, len, chunk, encoding, cb);
	      entry = entry.next;
	      state.bufferedRequestCount--;
	      // if we didn't call the onwrite immediately, then
	      // it means that we need to wait until it does.
	      // also, that means that the chunk and cb are currently
	      // being processed, so move the buffer counter past them.
	      if (state.writing) {
	        break;
	      }
	    }

	    if (entry === null) state.lastBufferedRequest = null;
	  }

	  state.bufferedRequest = entry;
	  state.bufferProcessing = false;
	}

	Writable.prototype._write = function (chunk, encoding, cb) {
	  cb(new Error('_write() is not implemented'));
	};

	Writable.prototype._writev = null;

	Writable.prototype.end = function (chunk, encoding, cb) {
	  var state = this._writableState;

	  if (typeof chunk === 'function') {
	    cb = chunk;
	    chunk = null;
	    encoding = null;
	  } else if (typeof encoding === 'function') {
	    cb = encoding;
	    encoding = null;
	  }

	  if (chunk !== null && chunk !== undefined) this.write(chunk, encoding);

	  // .end() fully uncorks
	  if (state.corked) {
	    state.corked = 1;
	    this.uncork();
	  }

	  // ignore unnecessary end() calls.
	  if (!state.ending && !state.finished) endWritable(this, state, cb);
	};

	function needFinish(state) {
	  return state.ending && state.length === 0 && state.bufferedRequest === null && !state.finished && !state.writing;
	}
	function callFinal(stream, state) {
	  stream._final(function (err) {
	    state.pendingcb--;
	    if (err) {
	      stream.emit('error', err);
	    }
	    state.prefinished = true;
	    stream.emit('prefinish');
	    finishMaybe(stream, state);
	  });
	}
	function prefinish(stream, state) {
	  if (!state.prefinished && !state.finalCalled) {
	    if (typeof stream._final === 'function') {
	      state.pendingcb++;
	      state.finalCalled = true;
	      pna.nextTick(callFinal, stream, state);
	    } else {
	      state.prefinished = true;
	      stream.emit('prefinish');
	    }
	  }
	}

	function finishMaybe(stream, state) {
	  var need = needFinish(state);
	  if (need) {
	    prefinish(stream, state);
	    if (state.pendingcb === 0) {
	      state.finished = true;
	      stream.emit('finish');
	    }
	  }
	  return need;
	}

	function endWritable(stream, state, cb) {
	  state.ending = true;
	  finishMaybe(stream, state);
	  if (cb) {
	    if (state.finished) pna.nextTick(cb);else stream.once('finish', cb);
	  }
	  state.ended = true;
	  stream.writable = false;
	}

	function onCorkedFinish(corkReq, state, err) {
	  var entry = corkReq.entry;
	  corkReq.entry = null;
	  while (entry) {
	    var cb = entry.callback;
	    state.pendingcb--;
	    cb(err);
	    entry = entry.next;
	  }
	  if (state.corkedRequestsFree) {
	    state.corkedRequestsFree.next = corkReq;
	  } else {
	    state.corkedRequestsFree = corkReq;
	  }
	}

	Object.defineProperty(Writable.prototype, 'destroyed', {
	  get: function () {
	    if (this._writableState === undefined) {
	      return false;
	    }
	    return this._writableState.destroyed;
	  },
	  set: function (value) {
	    // we ignore the value if the stream
	    // has not been initialized yet
	    if (!this._writableState) {
	      return;
	    }

	    // backward compatibility, the user is explicitly
	    // managing destroyed
	    this._writableState.destroyed = value;
	  }
	});

	Writable.prototype.destroy = destroyImpl.destroy;
	Writable.prototype._undestroy = destroyImpl.undestroy;
	Writable.prototype._destroy = function (err, cb) {
	  this.end();
	  cb(err);
	};
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(18), __webpack_require__(49).setImmediate, (function() { return this; }())))

/***/ }),
/* 88 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	var Buffer = __webpack_require__(41).Buffer;

	var isEncoding = Buffer.isEncoding || function (encoding) {
	  encoding = '' + encoding;
	  switch (encoding && encoding.toLowerCase()) {
	    case 'hex':case 'utf8':case 'utf-8':case 'ascii':case 'binary':case 'base64':case 'ucs2':case 'ucs-2':case 'utf16le':case 'utf-16le':case 'raw':
	      return true;
	    default:
	      return false;
	  }
	};

	function _normalizeEncoding(enc) {
	  if (!enc) return 'utf8';
	  var retried;
	  while (true) {
	    switch (enc) {
	      case 'utf8':
	      case 'utf-8':
	        return 'utf8';
	      case 'ucs2':
	      case 'ucs-2':
	      case 'utf16le':
	      case 'utf-16le':
	        return 'utf16le';
	      case 'latin1':
	      case 'binary':
	        return 'latin1';
	      case 'base64':
	      case 'ascii':
	      case 'hex':
	        return enc;
	      default:
	        if (retried) return; // undefined
	        enc = ('' + enc).toLowerCase();
	        retried = true;
	    }
	  }
	};

	// Do not cache `Buffer.isEncoding` when checking encoding names as some
	// modules monkey-patch it to support additional encodings
	function normalizeEncoding(enc) {
	  var nenc = _normalizeEncoding(enc);
	  if (typeof nenc !== 'string' && (Buffer.isEncoding === isEncoding || !isEncoding(enc))) throw new Error('Unknown encoding: ' + enc);
	  return nenc || enc;
	}

	// StringDecoder provides an interface for efficiently splitting a series of
	// buffers into a series of JS strings without breaking apart multi-byte
	// characters.
	exports.StringDecoder = StringDecoder;
	function StringDecoder(encoding) {
	  this.encoding = normalizeEncoding(encoding);
	  var nb;
	  switch (this.encoding) {
	    case 'utf16le':
	      this.text = utf16Text;
	      this.end = utf16End;
	      nb = 4;
	      break;
	    case 'utf8':
	      this.fillLast = utf8FillLast;
	      nb = 4;
	      break;
	    case 'base64':
	      this.text = base64Text;
	      this.end = base64End;
	      nb = 3;
	      break;
	    default:
	      this.write = simpleWrite;
	      this.end = simpleEnd;
	      return;
	  }
	  this.lastNeed = 0;
	  this.lastTotal = 0;
	  this.lastChar = Buffer.allocUnsafe(nb);
	}

	StringDecoder.prototype.write = function (buf) {
	  if (buf.length === 0) return '';
	  var r;
	  var i;
	  if (this.lastNeed) {
	    r = this.fillLast(buf);
	    if (r === undefined) return '';
	    i = this.lastNeed;
	    this.lastNeed = 0;
	  } else {
	    i = 0;
	  }
	  if (i < buf.length) return r ? r + this.text(buf, i) : this.text(buf, i);
	  return r || '';
	};

	StringDecoder.prototype.end = utf8End;

	// Returns only complete characters in a Buffer
	StringDecoder.prototype.text = utf8Text;

	// Attempts to complete a partial non-UTF-8 character using bytes from a Buffer
	StringDecoder.prototype.fillLast = function (buf) {
	  if (this.lastNeed <= buf.length) {
	    buf.copy(this.lastChar, this.lastTotal - this.lastNeed, 0, this.lastNeed);
	    return this.lastChar.toString(this.encoding, 0, this.lastTotal);
	  }
	  buf.copy(this.lastChar, this.lastTotal - this.lastNeed, 0, buf.length);
	  this.lastNeed -= buf.length;
	};

	// Checks the type of a UTF-8 byte, whether it's ASCII, a leading byte, or a
	// continuation byte.
	function utf8CheckByte(byte) {
	  if (byte <= 0x7F) return 0;else if (byte >> 5 === 0x06) return 2;else if (byte >> 4 === 0x0E) return 3;else if (byte >> 3 === 0x1E) return 4;
	  return -1;
	}

	// Checks at most 3 bytes at the end of a Buffer in order to detect an
	// incomplete multi-byte UTF-8 character. The total number of bytes (2, 3, or 4)
	// needed to complete the UTF-8 character (if applicable) are returned.
	function utf8CheckIncomplete(self, buf, i) {
	  var j = buf.length - 1;
	  if (j < i) return 0;
	  var nb = utf8CheckByte(buf[j]);
	  if (nb >= 0) {
	    if (nb > 0) self.lastNeed = nb - 1;
	    return nb;
	  }
	  if (--j < i) return 0;
	  nb = utf8CheckByte(buf[j]);
	  if (nb >= 0) {
	    if (nb > 0) self.lastNeed = nb - 2;
	    return nb;
	  }
	  if (--j < i) return 0;
	  nb = utf8CheckByte(buf[j]);
	  if (nb >= 0) {
	    if (nb > 0) {
	      if (nb === 2) nb = 0;else self.lastNeed = nb - 3;
	    }
	    return nb;
	  }
	  return 0;
	}

	// Validates as many continuation bytes for a multi-byte UTF-8 character as
	// needed or are available. If we see a non-continuation byte where we expect
	// one, we "replace" the validated continuation bytes we've seen so far with
	// UTF-8 replacement characters ('\ufffd'), to match v8's UTF-8 decoding
	// behavior. The continuation byte check is included three times in the case
	// where all of the continuation bytes for a character exist in the same buffer.
	// It is also done this way as a slight performance increase instead of using a
	// loop.
	function utf8CheckExtraBytes(self, buf, p) {
	  if ((buf[0] & 0xC0) !== 0x80) {
	    self.lastNeed = 0;
	    return '\ufffd'.repeat(p);
	  }
	  if (self.lastNeed > 1 && buf.length > 1) {
	    if ((buf[1] & 0xC0) !== 0x80) {
	      self.lastNeed = 1;
	      return '\ufffd'.repeat(p + 1);
	    }
	    if (self.lastNeed > 2 && buf.length > 2) {
	      if ((buf[2] & 0xC0) !== 0x80) {
	        self.lastNeed = 2;
	        return '\ufffd'.repeat(p + 2);
	      }
	    }
	  }
	}

	// Attempts to complete a multi-byte UTF-8 character using bytes from a Buffer.
	function utf8FillLast(buf) {
	  var p = this.lastTotal - this.lastNeed;
	  var r = utf8CheckExtraBytes(this, buf, p);
	  if (r !== undefined) return r;
	  if (this.lastNeed <= buf.length) {
	    buf.copy(this.lastChar, p, 0, this.lastNeed);
	    return this.lastChar.toString(this.encoding, 0, this.lastTotal);
	  }
	  buf.copy(this.lastChar, p, 0, buf.length);
	  this.lastNeed -= buf.length;
	}

	// Returns all complete UTF-8 characters in a Buffer. If the Buffer ended on a
	// partial character, the character's bytes are buffered until the required
	// number of bytes are available.
	function utf8Text(buf, i) {
	  var total = utf8CheckIncomplete(this, buf, i);
	  if (!this.lastNeed) return buf.toString('utf8', i);
	  this.lastTotal = total;
	  var end = buf.length - (total - this.lastNeed);
	  buf.copy(this.lastChar, 0, end);
	  return buf.toString('utf8', i, end);
	}

	// For UTF-8, a replacement character for each buffered byte of a (partial)
	// character needs to be added to the output.
	function utf8End(buf) {
	  var r = buf && buf.length ? this.write(buf) : '';
	  if (this.lastNeed) return r + '\ufffd'.repeat(this.lastTotal - this.lastNeed);
	  return r;
	}

	// UTF-16LE typically needs two bytes per character, but even if we have an even
	// number of bytes available, we need to check if we end on a leading/high
	// surrogate. In that case, we need to wait for the next two bytes in order to
	// decode the last character properly.
	function utf16Text(buf, i) {
	  if ((buf.length - i) % 2 === 0) {
	    var r = buf.toString('utf16le', i);
	    if (r) {
	      var c = r.charCodeAt(r.length - 1);
	      if (c >= 0xD800 && c <= 0xDBFF) {
	        this.lastNeed = 2;
	        this.lastTotal = 4;
	        this.lastChar[0] = buf[buf.length - 2];
	        this.lastChar[1] = buf[buf.length - 1];
	        return r.slice(0, -1);
	      }
	    }
	    return r;
	  }
	  this.lastNeed = 1;
	  this.lastTotal = 2;
	  this.lastChar[0] = buf[buf.length - 1];
	  return buf.toString('utf16le', i, buf.length - 1);
	}

	// For UTF-16LE we do not explicitly append special replacement characters if we
	// end on a partial character, we simply let v8 handle that.
	function utf16End(buf) {
	  var r = buf && buf.length ? this.write(buf) : '';
	  if (this.lastNeed) {
	    var end = this.lastTotal - this.lastNeed;
	    return r + this.lastChar.toString('utf16le', 0, end);
	  }
	  return r;
	}

	function base64Text(buf, i) {
	  var n = (buf.length - i) % 3;
	  if (n === 0) return buf.toString('base64', i);
	  this.lastNeed = 3 - n;
	  this.lastTotal = 3;
	  if (n === 1) {
	    this.lastChar[0] = buf[buf.length - 1];
	  } else {
	    this.lastChar[0] = buf[buf.length - 2];
	    this.lastChar[1] = buf[buf.length - 1];
	  }
	  return buf.toString('base64', i, buf.length - n);
	}

	function base64End(buf) {
	  var r = buf && buf.length ? this.write(buf) : '';
	  if (this.lastNeed) return r + this.lastChar.toString('base64', 0, 3 - this.lastNeed);
	  return r;
	}

	// Pass bytes on through for single-byte encodings (e.g. ascii, latin1, hex)
	function simpleWrite(buf) {
	  return buf.toString(this.encoding);
	}

	function simpleEnd(buf) {
	  return buf && buf.length ? this.write(buf) : '';
	}

/***/ }),
/* 89 */
/***/ (function(module, exports, __webpack_require__) {

	// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	// a transform stream is a readable/writable stream where you do
	// something with the data.  Sometimes it's called a "filter",
	// but that's not a great name for it, since that implies a thing where
	// some bits pass through, and others are simply ignored.  (That would
	// be a valid example of a transform, of course.)
	//
	// While the output is causally related to the input, it's not a
	// necessarily symmetric or synchronous transformation.  For example,
	// a zlib stream might take multiple plain-text writes(), and then
	// emit a single compressed chunk some time in the future.
	//
	// Here's how this works:
	//
	// The Transform stream has all the aspects of the readable and writable
	// stream classes.  When you write(chunk), that calls _write(chunk,cb)
	// internally, and returns false if there's a lot of pending writes
	// buffered up.  When you call read(), that calls _read(n) until
	// there's enough pending readable data buffered up.
	//
	// In a transform stream, the written data is placed in a buffer.  When
	// _read(n) is called, it transforms the queued up data, calling the
	// buffered _write cb's as it consumes chunks.  If consuming a single
	// written chunk would result in multiple output chunks, then the first
	// outputted bit calls the readcb, and subsequent chunks just go into
	// the read buffer, and will cause it to emit 'readable' if necessary.
	//
	// This way, back-pressure is actually determined by the reading side,
	// since _read has to be called to start processing a new chunk.  However,
	// a pathological inflate type of transform can cause excessive buffering
	// here.  For example, imagine a stream where every byte of input is
	// interpreted as an integer from 0-255, and then results in that many
	// bytes of output.  Writing the 4 bytes {ff,ff,ff,ff} would result in
	// 1kb of data being output.  In this case, you could write a very small
	// amount of input, and end up with a very large amount of output.  In
	// such a pathological inflating mechanism, there'd be no way to tell
	// the system to stop doing the transform.  A single 4MB write could
	// cause the system to run out of memory.
	//
	// However, even in such a pathological case, only a single written chunk
	// would be consumed, and then the rest would wait (un-transformed) until
	// the results of the previous transformed chunk were consumed.

	'use strict';

	module.exports = Transform;

	var Duplex = __webpack_require__(86);

	/*<replacement>*/
	var util = __webpack_require__(42);
	util.inherits = __webpack_require__(35);
	/*</replacement>*/

	util.inherits(Transform, Duplex);

	function afterTransform(er, data) {
	  var ts = this._transformState;
	  ts.transforming = false;

	  var cb = ts.writecb;

	  if (!cb) {
	    return this.emit('error', new Error('write callback called multiple times'));
	  }

	  ts.writechunk = null;
	  ts.writecb = null;

	  if (data != null) // single equals check for both `null` and `undefined`
	    this.push(data);

	  cb(er);

	  var rs = this._readableState;
	  rs.reading = false;
	  if (rs.needReadable || rs.length < rs.highWaterMark) {
	    this._read(rs.highWaterMark);
	  }
	}

	function Transform(options) {
	  if (!(this instanceof Transform)) return new Transform(options);

	  Duplex.call(this, options);

	  this._transformState = {
	    afterTransform: afterTransform.bind(this),
	    needTransform: false,
	    transforming: false,
	    writecb: null,
	    writechunk: null,
	    writeencoding: null
	  };

	  // start out asking for a readable event once data is transformed.
	  this._readableState.needReadable = true;

	  // we have implemented the _read method, and done the other things
	  // that Readable wants before the first _read call, so unset the
	  // sync guard flag.
	  this._readableState.sync = false;

	  if (options) {
	    if (typeof options.transform === 'function') this._transform = options.transform;

	    if (typeof options.flush === 'function') this._flush = options.flush;
	  }

	  // When the writable side finishes, then flush out anything remaining.
	  this.on('prefinish', prefinish);
	}

	function prefinish() {
	  var _this = this;

	  if (typeof this._flush === 'function') {
	    this._flush(function (er, data) {
	      done(_this, er, data);
	    });
	  } else {
	    done(this, null, null);
	  }
	}

	Transform.prototype.push = function (chunk, encoding) {
	  this._transformState.needTransform = false;
	  return Duplex.prototype.push.call(this, chunk, encoding);
	};

	// This is the part where you do stuff!
	// override this function in implementation classes.
	// 'chunk' is an input chunk.
	//
	// Call `push(newChunk)` to pass along transformed output
	// to the readable side.  You may call 'push' zero or more times.
	//
	// Call `cb(err)` when you are done with this chunk.  If you pass
	// an error, then that'll put the hurt on the whole operation.  If you
	// never call cb(), then you'll never get another chunk.
	Transform.prototype._transform = function (chunk, encoding, cb) {
	  throw new Error('_transform() is not implemented');
	};

	Transform.prototype._write = function (chunk, encoding, cb) {
	  var ts = this._transformState;
	  ts.writecb = cb;
	  ts.writechunk = chunk;
	  ts.writeencoding = encoding;
	  if (!ts.transforming) {
	    var rs = this._readableState;
	    if (ts.needTransform || rs.needReadable || rs.length < rs.highWaterMark) this._read(rs.highWaterMark);
	  }
	};

	// Doesn't matter what the args are here.
	// _transform does all the work.
	// That we got here means that the readable side wants more data.
	Transform.prototype._read = function (n) {
	  var ts = this._transformState;

	  if (ts.writechunk !== null && ts.writecb && !ts.transforming) {
	    ts.transforming = true;
	    this._transform(ts.writechunk, ts.writeencoding, ts.afterTransform);
	  } else {
	    // mark that we need a transform, so that any data that comes in
	    // will get processed, now that we've asked for it.
	    ts.needTransform = true;
	  }
	};

	Transform.prototype._destroy = function (err, cb) {
	  var _this2 = this;

	  Duplex.prototype._destroy.call(this, err, function (err2) {
	    cb(err2);
	    _this2.emit('close');
	  });
	};

	function done(stream, er, data) {
	  if (er) return stream.emit('error', er);

	  if (data != null) // single equals check for both `null` and `undefined`
	    stream.push(data);

	  // if there's nothing in the write buffer, then that means
	  // that nothing more will ever be provided
	  if (stream._writableState.length) throw new Error('Calling transform done when ws.length != 0');

	  if (stream._transformState.transforming) throw new Error('Calling transform done when still transforming');

	  return stream.push(null);
	}

/***/ }),
/* 90 */
/***/ (function(module, exports, __webpack_require__) {

	// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	// a passthrough stream.
	// basically just the most minimal sort of Transform stream.
	// Every written chunk gets output as-is.

	'use strict';

	module.exports = PassThrough;

	var Transform = __webpack_require__(89);

	/*<replacement>*/
	var util = __webpack_require__(42);
	util.inherits = __webpack_require__(35);
	/*</replacement>*/

	util.inherits(PassThrough, Transform);

	function PassThrough(options) {
	  if (!(this instanceof PassThrough)) return new PassThrough(options);

	  Transform.call(this, options);
	}

	PassThrough.prototype._transform = function (chunk, encoding, cb) {
	  cb(null, chunk);
	};

/***/ }),
/* 91 */
/***/ (function(module, exports, __webpack_require__) {

	var Buffer = __webpack_require__(5).Buffer

	module.exports = function (buf) {
		// If the buffer is backed by a Uint8Array, a faster version will work
		if (buf instanceof Uint8Array) {
			// If the buffer isn't a subarray, return the underlying ArrayBuffer
			if (buf.byteOffset === 0 && buf.byteLength === buf.buffer.byteLength) {
				return buf.buffer
			} else if (typeof buf.buffer.slice === 'function') {
				// Otherwise we need to get a proper copy
				return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength)
			}
		}

		if (Buffer.isBuffer(buf)) {
			// This is the slow version that will work with any Buffer
			// implementation (even in old browsers)
			var arrayCopy = new Uint8Array(buf.length)
			var len = buf.length
			for (var i = 0; i < len; i++) {
				arrayCopy[i] = buf[i]
			}
			return arrayCopy.buffer
		} else {
			throw new Error('Argument must be a Buffer')
		}
	}


/***/ }),
/* 92 */
/***/ (function(module, exports) {

	module.exports = extend

	var hasOwnProperty = Object.prototype.hasOwnProperty;

	function extend() {
	    var target = {}

	    for (var i = 0; i < arguments.length; i++) {
	        var source = arguments[i]

	        for (var key in source) {
	            if (hasOwnProperty.call(source, key)) {
	                target[key] = source[key]
	            }
	        }
	    }

	    return target
	}


/***/ }),
/* 93 */
/***/ (function(module, exports) {

	module.exports = {
	  "100": "Continue",
	  "101": "Switching Protocols",
	  "102": "Processing",
	  "200": "OK",
	  "201": "Created",
	  "202": "Accepted",
	  "203": "Non-Authoritative Information",
	  "204": "No Content",
	  "205": "Reset Content",
	  "206": "Partial Content",
	  "207": "Multi-Status",
	  "208": "Already Reported",
	  "226": "IM Used",
	  "300": "Multiple Choices",
	  "301": "Moved Permanently",
	  "302": "Found",
	  "303": "See Other",
	  "304": "Not Modified",
	  "305": "Use Proxy",
	  "307": "Temporary Redirect",
	  "308": "Permanent Redirect",
	  "400": "Bad Request",
	  "401": "Unauthorized",
	  "402": "Payment Required",
	  "403": "Forbidden",
	  "404": "Not Found",
	  "405": "Method Not Allowed",
	  "406": "Not Acceptable",
	  "407": "Proxy Authentication Required",
	  "408": "Request Timeout",
	  "409": "Conflict",
	  "410": "Gone",
	  "411": "Length Required",
	  "412": "Precondition Failed",
	  "413": "Payload Too Large",
	  "414": "URI Too Long",
	  "415": "Unsupported Media Type",
	  "416": "Range Not Satisfiable",
	  "417": "Expectation Failed",
	  "418": "I'm a teapot",
	  "421": "Misdirected Request",
	  "422": "Unprocessable Entity",
	  "423": "Locked",
	  "424": "Failed Dependency",
	  "425": "Unordered Collection",
	  "426": "Upgrade Required",
	  "428": "Precondition Required",
	  "429": "Too Many Requests",
	  "431": "Request Header Fields Too Large",
	  "451": "Unavailable For Legal Reasons",
	  "500": "Internal Server Error",
	  "501": "Not Implemented",
	  "502": "Bad Gateway",
	  "503": "Service Unavailable",
	  "504": "Gateway Timeout",
	  "505": "HTTP Version Not Supported",
	  "506": "Variant Also Negotiates",
	  "507": "Insufficient Storage",
	  "508": "Loop Detected",
	  "509": "Bandwidth Limit Exceeded",
	  "510": "Not Extended",
	  "511": "Network Authentication Required"
	}


/***/ }),
/* 94 */
/***/ (function(module, exports, __webpack_require__) {

	// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	'use strict';

	var punycode = __webpack_require__(95);
	var util = __webpack_require__(97);

	exports.parse = urlParse;
	exports.resolve = urlResolve;
	exports.resolveObject = urlResolveObject;
	exports.format = urlFormat;

	exports.Url = Url;

	function Url() {
	  this.protocol = null;
	  this.slashes = null;
	  this.auth = null;
	  this.host = null;
	  this.port = null;
	  this.hostname = null;
	  this.hash = null;
	  this.search = null;
	  this.query = null;
	  this.pathname = null;
	  this.path = null;
	  this.href = null;
	}

	// Reference: RFC 3986, RFC 1808, RFC 2396

	// define these here so at least they only have to be
	// compiled once on the first module load.
	var protocolPattern = /^([a-z0-9.+-]+:)/i,
	    portPattern = /:[0-9]*$/,

	    // Special case for a simple path URL
	    simplePathPattern = /^(\/\/?(?!\/)[^\?\s]*)(\?[^\s]*)?$/,

	    // RFC 2396: characters reserved for delimiting URLs.
	    // We actually just auto-escape these.
	    delims = ['<', '>', '"', '`', ' ', '\r', '\n', '\t'],

	    // RFC 2396: characters not allowed for various reasons.
	    unwise = ['{', '}', '|', '\\', '^', '`'].concat(delims),

	    // Allowed by RFCs, but cause of XSS attacks.  Always escape these.
	    autoEscape = ['\''].concat(unwise),
	    // Characters that are never ever allowed in a hostname.
	    // Note that any invalid chars are also handled, but these
	    // are the ones that are *expected* to be seen, so we fast-path
	    // them.
	    nonHostChars = ['%', '/', '?', ';', '#'].concat(autoEscape),
	    hostEndingChars = ['/', '?', '#'],
	    hostnameMaxLen = 255,
	    hostnamePartPattern = /^[+a-z0-9A-Z_-]{0,63}$/,
	    hostnamePartStart = /^([+a-z0-9A-Z_-]{0,63})(.*)$/,
	    // protocols that can allow "unsafe" and "unwise" chars.
	    unsafeProtocol = {
	      'javascript': true,
	      'javascript:': true
	    },
	    // protocols that never have a hostname.
	    hostlessProtocol = {
	      'javascript': true,
	      'javascript:': true
	    },
	    // protocols that always contain a // bit.
	    slashedProtocol = {
	      'http': true,
	      'https': true,
	      'ftp': true,
	      'gopher': true,
	      'file': true,
	      'http:': true,
	      'https:': true,
	      'ftp:': true,
	      'gopher:': true,
	      'file:': true
	    },
	    querystring = __webpack_require__(98);

	function urlParse(url, parseQueryString, slashesDenoteHost) {
	  if (url && util.isObject(url) && url instanceof Url) return url;

	  var u = new Url;
	  u.parse(url, parseQueryString, slashesDenoteHost);
	  return u;
	}

	Url.prototype.parse = function(url, parseQueryString, slashesDenoteHost) {
	  if (!util.isString(url)) {
	    throw new TypeError("Parameter 'url' must be a string, not " + typeof url);
	  }

	  // Copy chrome, IE, opera backslash-handling behavior.
	  // Back slashes before the query string get converted to forward slashes
	  // See: https://code.google.com/p/chromium/issues/detail?id=25916
	  var queryIndex = url.indexOf('?'),
	      splitter =
	          (queryIndex !== -1 && queryIndex < url.indexOf('#')) ? '?' : '#',
	      uSplit = url.split(splitter),
	      slashRegex = /\\/g;
	  uSplit[0] = uSplit[0].replace(slashRegex, '/');
	  url = uSplit.join(splitter);

	  var rest = url;

	  // trim before proceeding.
	  // This is to support parse stuff like "  http://foo.com  \n"
	  rest = rest.trim();

	  if (!slashesDenoteHost && url.split('#').length === 1) {
	    // Try fast path regexp
	    var simplePath = simplePathPattern.exec(rest);
	    if (simplePath) {
	      this.path = rest;
	      this.href = rest;
	      this.pathname = simplePath[1];
	      if (simplePath[2]) {
	        this.search = simplePath[2];
	        if (parseQueryString) {
	          this.query = querystring.parse(this.search.substr(1));
	        } else {
	          this.query = this.search.substr(1);
	        }
	      } else if (parseQueryString) {
	        this.search = '';
	        this.query = {};
	      }
	      return this;
	    }
	  }

	  var proto = protocolPattern.exec(rest);
	  if (proto) {
	    proto = proto[0];
	    var lowerProto = proto.toLowerCase();
	    this.protocol = lowerProto;
	    rest = rest.substr(proto.length);
	  }

	  // figure out if it's got a host
	  // user@server is *always* interpreted as a hostname, and url
	  // resolution will treat //foo/bar as host=foo,path=bar because that's
	  // how the browser resolves relative URLs.
	  if (slashesDenoteHost || proto || rest.match(/^\/\/[^@\/]+@[^@\/]+/)) {
	    var slashes = rest.substr(0, 2) === '//';
	    if (slashes && !(proto && hostlessProtocol[proto])) {
	      rest = rest.substr(2);
	      this.slashes = true;
	    }
	  }

	  if (!hostlessProtocol[proto] &&
	      (slashes || (proto && !slashedProtocol[proto]))) {

	    // there's a hostname.
	    // the first instance of /, ?, ;, or # ends the host.
	    //
	    // If there is an @ in the hostname, then non-host chars *are* allowed
	    // to the left of the last @ sign, unless some host-ending character
	    // comes *before* the @-sign.
	    // URLs are obnoxious.
	    //
	    // ex:
	    // http://a@b@c/ => user:a@b host:c
	    // http://a@b?@c => user:a host:c path:/?@c

	    // v0.12 TODO(isaacs): This is not quite how Chrome does things.
	    // Review our test case against browsers more comprehensively.

	    // find the first instance of any hostEndingChars
	    var hostEnd = -1;
	    for (var i = 0; i < hostEndingChars.length; i++) {
	      var hec = rest.indexOf(hostEndingChars[i]);
	      if (hec !== -1 && (hostEnd === -1 || hec < hostEnd))
	        hostEnd = hec;
	    }

	    // at this point, either we have an explicit point where the
	    // auth portion cannot go past, or the last @ char is the decider.
	    var auth, atSign;
	    if (hostEnd === -1) {
	      // atSign can be anywhere.
	      atSign = rest.lastIndexOf('@');
	    } else {
	      // atSign must be in auth portion.
	      // http://a@b/c@d => host:b auth:a path:/c@d
	      atSign = rest.lastIndexOf('@', hostEnd);
	    }

	    // Now we have a portion which is definitely the auth.
	    // Pull that off.
	    if (atSign !== -1) {
	      auth = rest.slice(0, atSign);
	      rest = rest.slice(atSign + 1);
	      this.auth = decodeURIComponent(auth);
	    }

	    // the host is the remaining to the left of the first non-host char
	    hostEnd = -1;
	    for (var i = 0; i < nonHostChars.length; i++) {
	      var hec = rest.indexOf(nonHostChars[i]);
	      if (hec !== -1 && (hostEnd === -1 || hec < hostEnd))
	        hostEnd = hec;
	    }
	    // if we still have not hit it, then the entire thing is a host.
	    if (hostEnd === -1)
	      hostEnd = rest.length;

	    this.host = rest.slice(0, hostEnd);
	    rest = rest.slice(hostEnd);

	    // pull out port.
	    this.parseHost();

	    // we've indicated that there is a hostname,
	    // so even if it's empty, it has to be present.
	    this.hostname = this.hostname || '';

	    // if hostname begins with [ and ends with ]
	    // assume that it's an IPv6 address.
	    var ipv6Hostname = this.hostname[0] === '[' &&
	        this.hostname[this.hostname.length - 1] === ']';

	    // validate a little.
	    if (!ipv6Hostname) {
	      var hostparts = this.hostname.split(/\./);
	      for (var i = 0, l = hostparts.length; i < l; i++) {
	        var part = hostparts[i];
	        if (!part) continue;
	        if (!part.match(hostnamePartPattern)) {
	          var newpart = '';
	          for (var j = 0, k = part.length; j < k; j++) {
	            if (part.charCodeAt(j) > 127) {
	              // we replace non-ASCII char with a temporary placeholder
	              // we need this to make sure size of hostname is not
	              // broken by replacing non-ASCII by nothing
	              newpart += 'x';
	            } else {
	              newpart += part[j];
	            }
	          }
	          // we test again with ASCII char only
	          if (!newpart.match(hostnamePartPattern)) {
	            var validParts = hostparts.slice(0, i);
	            var notHost = hostparts.slice(i + 1);
	            var bit = part.match(hostnamePartStart);
	            if (bit) {
	              validParts.push(bit[1]);
	              notHost.unshift(bit[2]);
	            }
	            if (notHost.length) {
	              rest = '/' + notHost.join('.') + rest;
	            }
	            this.hostname = validParts.join('.');
	            break;
	          }
	        }
	      }
	    }

	    if (this.hostname.length > hostnameMaxLen) {
	      this.hostname = '';
	    } else {
	      // hostnames are always lower case.
	      this.hostname = this.hostname.toLowerCase();
	    }

	    if (!ipv6Hostname) {
	      // IDNA Support: Returns a punycoded representation of "domain".
	      // It only converts parts of the domain name that
	      // have non-ASCII characters, i.e. it doesn't matter if
	      // you call it with a domain that already is ASCII-only.
	      this.hostname = punycode.toASCII(this.hostname);
	    }

	    var p = this.port ? ':' + this.port : '';
	    var h = this.hostname || '';
	    this.host = h + p;
	    this.href += this.host;

	    // strip [ and ] from the hostname
	    // the host field still retains them, though
	    if (ipv6Hostname) {
	      this.hostname = this.hostname.substr(1, this.hostname.length - 2);
	      if (rest[0] !== '/') {
	        rest = '/' + rest;
	      }
	    }
	  }

	  // now rest is set to the post-host stuff.
	  // chop off any delim chars.
	  if (!unsafeProtocol[lowerProto]) {

	    // First, make 100% sure that any "autoEscape" chars get
	    // escaped, even if encodeURIComponent doesn't think they
	    // need to be.
	    for (var i = 0, l = autoEscape.length; i < l; i++) {
	      var ae = autoEscape[i];
	      if (rest.indexOf(ae) === -1)
	        continue;
	      var esc = encodeURIComponent(ae);
	      if (esc === ae) {
	        esc = escape(ae);
	      }
	      rest = rest.split(ae).join(esc);
	    }
	  }


	  // chop off from the tail first.
	  var hash = rest.indexOf('#');
	  if (hash !== -1) {
	    // got a fragment string.
	    this.hash = rest.substr(hash);
	    rest = rest.slice(0, hash);
	  }
	  var qm = rest.indexOf('?');
	  if (qm !== -1) {
	    this.search = rest.substr(qm);
	    this.query = rest.substr(qm + 1);
	    if (parseQueryString) {
	      this.query = querystring.parse(this.query);
	    }
	    rest = rest.slice(0, qm);
	  } else if (parseQueryString) {
	    // no query string, but parseQueryString still requested
	    this.search = '';
	    this.query = {};
	  }
	  if (rest) this.pathname = rest;
	  if (slashedProtocol[lowerProto] &&
	      this.hostname && !this.pathname) {
	    this.pathname = '/';
	  }

	  //to support http.request
	  if (this.pathname || this.search) {
	    var p = this.pathname || '';
	    var s = this.search || '';
	    this.path = p + s;
	  }

	  // finally, reconstruct the href based on what has been validated.
	  this.href = this.format();
	  return this;
	};

	// format a parsed object into a url string
	function urlFormat(obj) {
	  // ensure it's an object, and not a string url.
	  // If it's an obj, this is a no-op.
	  // this way, you can call url_format() on strings
	  // to clean up potentially wonky urls.
	  if (util.isString(obj)) obj = urlParse(obj);
	  if (!(obj instanceof Url)) return Url.prototype.format.call(obj);
	  return obj.format();
	}

	Url.prototype.format = function() {
	  var auth = this.auth || '';
	  if (auth) {
	    auth = encodeURIComponent(auth);
	    auth = auth.replace(/%3A/i, ':');
	    auth += '@';
	  }

	  var protocol = this.protocol || '',
	      pathname = this.pathname || '',
	      hash = this.hash || '',
	      host = false,
	      query = '';

	  if (this.host) {
	    host = auth + this.host;
	  } else if (this.hostname) {
	    host = auth + (this.hostname.indexOf(':') === -1 ?
	        this.hostname :
	        '[' + this.hostname + ']');
	    if (this.port) {
	      host += ':' + this.port;
	    }
	  }

	  if (this.query &&
	      util.isObject(this.query) &&
	      Object.keys(this.query).length) {
	    query = querystring.stringify(this.query);
	  }

	  var search = this.search || (query && ('?' + query)) || '';

	  if (protocol && protocol.substr(-1) !== ':') protocol += ':';

	  // only the slashedProtocols get the //.  Not mailto:, xmpp:, etc.
	  // unless they had them to begin with.
	  if (this.slashes ||
	      (!protocol || slashedProtocol[protocol]) && host !== false) {
	    host = '//' + (host || '');
	    if (pathname && pathname.charAt(0) !== '/') pathname = '/' + pathname;
	  } else if (!host) {
	    host = '';
	  }

	  if (hash && hash.charAt(0) !== '#') hash = '#' + hash;
	  if (search && search.charAt(0) !== '?') search = '?' + search;

	  pathname = pathname.replace(/[?#]/g, function(match) {
	    return encodeURIComponent(match);
	  });
	  search = search.replace('#', '%23');

	  return protocol + host + pathname + search + hash;
	};

	function urlResolve(source, relative) {
	  return urlParse(source, false, true).resolve(relative);
	}

	Url.prototype.resolve = function(relative) {
	  return this.resolveObject(urlParse(relative, false, true)).format();
	};

	function urlResolveObject(source, relative) {
	  if (!source) return relative;
	  return urlParse(source, false, true).resolveObject(relative);
	}

	Url.prototype.resolveObject = function(relative) {
	  if (util.isString(relative)) {
	    var rel = new Url();
	    rel.parse(relative, false, true);
	    relative = rel;
	  }

	  var result = new Url();
	  var tkeys = Object.keys(this);
	  for (var tk = 0; tk < tkeys.length; tk++) {
	    var tkey = tkeys[tk];
	    result[tkey] = this[tkey];
	  }

	  // hash is always overridden, no matter what.
	  // even href="" will remove it.
	  result.hash = relative.hash;

	  // if the relative url is empty, then there's nothing left to do here.
	  if (relative.href === '') {
	    result.href = result.format();
	    return result;
	  }

	  // hrefs like //foo/bar always cut to the protocol.
	  if (relative.slashes && !relative.protocol) {
	    // take everything except the protocol from relative
	    var rkeys = Object.keys(relative);
	    for (var rk = 0; rk < rkeys.length; rk++) {
	      var rkey = rkeys[rk];
	      if (rkey !== 'protocol')
	        result[rkey] = relative[rkey];
	    }

	    //urlParse appends trailing / to urls like http://www.example.com
	    if (slashedProtocol[result.protocol] &&
	        result.hostname && !result.pathname) {
	      result.path = result.pathname = '/';
	    }

	    result.href = result.format();
	    return result;
	  }

	  if (relative.protocol && relative.protocol !== result.protocol) {
	    // if it's a known url protocol, then changing
	    // the protocol does weird things
	    // first, if it's not file:, then we MUST have a host,
	    // and if there was a path
	    // to begin with, then we MUST have a path.
	    // if it is file:, then the host is dropped,
	    // because that's known to be hostless.
	    // anything else is assumed to be absolute.
	    if (!slashedProtocol[relative.protocol]) {
	      var keys = Object.keys(relative);
	      for (var v = 0; v < keys.length; v++) {
	        var k = keys[v];
	        result[k] = relative[k];
	      }
	      result.href = result.format();
	      return result;
	    }

	    result.protocol = relative.protocol;
	    if (!relative.host && !hostlessProtocol[relative.protocol]) {
	      var relPath = (relative.pathname || '').split('/');
	      while (relPath.length && !(relative.host = relPath.shift()));
	      if (!relative.host) relative.host = '';
	      if (!relative.hostname) relative.hostname = '';
	      if (relPath[0] !== '') relPath.unshift('');
	      if (relPath.length < 2) relPath.unshift('');
	      result.pathname = relPath.join('/');
	    } else {
	      result.pathname = relative.pathname;
	    }
	    result.search = relative.search;
	    result.query = relative.query;
	    result.host = relative.host || '';
	    result.auth = relative.auth;
	    result.hostname = relative.hostname || relative.host;
	    result.port = relative.port;
	    // to support http.request
	    if (result.pathname || result.search) {
	      var p = result.pathname || '';
	      var s = result.search || '';
	      result.path = p + s;
	    }
	    result.slashes = result.slashes || relative.slashes;
	    result.href = result.format();
	    return result;
	  }

	  var isSourceAbs = (result.pathname && result.pathname.charAt(0) === '/'),
	      isRelAbs = (
	          relative.host ||
	          relative.pathname && relative.pathname.charAt(0) === '/'
	      ),
	      mustEndAbs = (isRelAbs || isSourceAbs ||
	                    (result.host && relative.pathname)),
	      removeAllDots = mustEndAbs,
	      srcPath = result.pathname && result.pathname.split('/') || [],
	      relPath = relative.pathname && relative.pathname.split('/') || [],
	      psychotic = result.protocol && !slashedProtocol[result.protocol];

	  // if the url is a non-slashed url, then relative
	  // links like ../.. should be able
	  // to crawl up to the hostname, as well.  This is strange.
	  // result.protocol has already been set by now.
	  // Later on, put the first path part into the host field.
	  if (psychotic) {
	    result.hostname = '';
	    result.port = null;
	    if (result.host) {
	      if (srcPath[0] === '') srcPath[0] = result.host;
	      else srcPath.unshift(result.host);
	    }
	    result.host = '';
	    if (relative.protocol) {
	      relative.hostname = null;
	      relative.port = null;
	      if (relative.host) {
	        if (relPath[0] === '') relPath[0] = relative.host;
	        else relPath.unshift(relative.host);
	      }
	      relative.host = null;
	    }
	    mustEndAbs = mustEndAbs && (relPath[0] === '' || srcPath[0] === '');
	  }

	  if (isRelAbs) {
	    // it's absolute.
	    result.host = (relative.host || relative.host === '') ?
	                  relative.host : result.host;
	    result.hostname = (relative.hostname || relative.hostname === '') ?
	                      relative.hostname : result.hostname;
	    result.search = relative.search;
	    result.query = relative.query;
	    srcPath = relPath;
	    // fall through to the dot-handling below.
	  } else if (relPath.length) {
	    // it's relative
	    // throw away the existing file, and take the new path instead.
	    if (!srcPath) srcPath = [];
	    srcPath.pop();
	    srcPath = srcPath.concat(relPath);
	    result.search = relative.search;
	    result.query = relative.query;
	  } else if (!util.isNullOrUndefined(relative.search)) {
	    // just pull out the search.
	    // like href='?foo'.
	    // Put this after the other two cases because it simplifies the booleans
	    if (psychotic) {
	      result.hostname = result.host = srcPath.shift();
	      //occationaly the auth can get stuck only in host
	      //this especially happens in cases like
	      //url.resolveObject('mailto:local1@domain1', 'local2@domain2')
	      var authInHost = result.host && result.host.indexOf('@') > 0 ?
	                       result.host.split('@') : false;
	      if (authInHost) {
	        result.auth = authInHost.shift();
	        result.host = result.hostname = authInHost.shift();
	      }
	    }
	    result.search = relative.search;
	    result.query = relative.query;
	    //to support http.request
	    if (!util.isNull(result.pathname) || !util.isNull(result.search)) {
	      result.path = (result.pathname ? result.pathname : '') +
	                    (result.search ? result.search : '');
	    }
	    result.href = result.format();
	    return result;
	  }

	  if (!srcPath.length) {
	    // no path at all.  easy.
	    // we've already handled the other stuff above.
	    result.pathname = null;
	    //to support http.request
	    if (result.search) {
	      result.path = '/' + result.search;
	    } else {
	      result.path = null;
	    }
	    result.href = result.format();
	    return result;
	  }

	  // if a url ENDs in . or .., then it must get a trailing slash.
	  // however, if it ends in anything else non-slashy,
	  // then it must NOT get a trailing slash.
	  var last = srcPath.slice(-1)[0];
	  var hasTrailingSlash = (
	      (result.host || relative.host || srcPath.length > 1) &&
	      (last === '.' || last === '..') || last === '');

	  // strip single dots, resolve double dots to parent dir
	  // if the path tries to go above the root, `up` ends up > 0
	  var up = 0;
	  for (var i = srcPath.length; i >= 0; i--) {
	    last = srcPath[i];
	    if (last === '.') {
	      srcPath.splice(i, 1);
	    } else if (last === '..') {
	      srcPath.splice(i, 1);
	      up++;
	    } else if (up) {
	      srcPath.splice(i, 1);
	      up--;
	    }
	  }

	  // if the path is allowed to go above the root, restore leading ..s
	  if (!mustEndAbs && !removeAllDots) {
	    for (; up--; up) {
	      srcPath.unshift('..');
	    }
	  }

	  if (mustEndAbs && srcPath[0] !== '' &&
	      (!srcPath[0] || srcPath[0].charAt(0) !== '/')) {
	    srcPath.unshift('');
	  }

	  if (hasTrailingSlash && (srcPath.join('/').substr(-1) !== '/')) {
	    srcPath.push('');
	  }

	  var isAbsolute = srcPath[0] === '' ||
	      (srcPath[0] && srcPath[0].charAt(0) === '/');

	  // put the host back
	  if (psychotic) {
	    result.hostname = result.host = isAbsolute ? '' :
	                                    srcPath.length ? srcPath.shift() : '';
	    //occationaly the auth can get stuck only in host
	    //this especially happens in cases like
	    //url.resolveObject('mailto:local1@domain1', 'local2@domain2')
	    var authInHost = result.host && result.host.indexOf('@') > 0 ?
	                     result.host.split('@') : false;
	    if (authInHost) {
	      result.auth = authInHost.shift();
	      result.host = result.hostname = authInHost.shift();
	    }
	  }

	  mustEndAbs = mustEndAbs || (result.host && srcPath.length);

	  if (mustEndAbs && !isAbsolute) {
	    srcPath.unshift('');
	  }

	  if (!srcPath.length) {
	    result.pathname = null;
	    result.path = null;
	  } else {
	    result.pathname = srcPath.join('/');
	  }

	  //to support request.http
	  if (!util.isNull(result.pathname) || !util.isNull(result.search)) {
	    result.path = (result.pathname ? result.pathname : '') +
	                  (result.search ? result.search : '');
	  }
	  result.auth = relative.auth || result.auth;
	  result.slashes = result.slashes || relative.slashes;
	  result.href = result.format();
	  return result;
	};

	Url.prototype.parseHost = function() {
	  var host = this.host;
	  var port = portPattern.exec(host);
	  if (port) {
	    port = port[0];
	    if (port !== ':') {
	      this.port = port.substr(1);
	    }
	    host = host.substr(0, host.length - port.length);
	  }
	  if (host) this.hostname = host;
	};


/***/ }),
/* 95 */
/***/ (function(module, exports, __webpack_require__) {

	var __WEBPACK_AMD_DEFINE_RESULT__;/* WEBPACK VAR INJECTION */(function(module, global) {/*! https://mths.be/punycode v1.3.2 by @mathias */
	;(function(root) {

		/** Detect free variables */
		var freeExports = typeof exports == 'object' && exports &&
			!exports.nodeType && exports;
		var freeModule = typeof module == 'object' && module &&
			!module.nodeType && module;
		var freeGlobal = typeof global == 'object' && global;
		if (
			freeGlobal.global === freeGlobal ||
			freeGlobal.window === freeGlobal ||
			freeGlobal.self === freeGlobal
		) {
			root = freeGlobal;
		}

		/**
		 * The `punycode` object.
		 * @name punycode
		 * @type Object
		 */
		var punycode,

		/** Highest positive signed 32-bit float value */
		maxInt = 2147483647, // aka. 0x7FFFFFFF or 2^31-1

		/** Bootstring parameters */
		base = 36,
		tMin = 1,
		tMax = 26,
		skew = 38,
		damp = 700,
		initialBias = 72,
		initialN = 128, // 0x80
		delimiter = '-', // '\x2D'

		/** Regular expressions */
		regexPunycode = /^xn--/,
		regexNonASCII = /[^\x20-\x7E]/, // unprintable ASCII chars + non-ASCII chars
		regexSeparators = /[\x2E\u3002\uFF0E\uFF61]/g, // RFC 3490 separators

		/** Error messages */
		errors = {
			'overflow': 'Overflow: input needs wider integers to process',
			'not-basic': 'Illegal input >= 0x80 (not a basic code point)',
			'invalid-input': 'Invalid input'
		},

		/** Convenience shortcuts */
		baseMinusTMin = base - tMin,
		floor = Math.floor,
		stringFromCharCode = String.fromCharCode,

		/** Temporary variable */
		key;

		/*--------------------------------------------------------------------------*/

		/**
		 * A generic error utility function.
		 * @private
		 * @param {String} type The error type.
		 * @returns {Error} Throws a `RangeError` with the applicable error message.
		 */
		function error(type) {
			throw RangeError(errors[type]);
		}

		/**
		 * A generic `Array#map` utility function.
		 * @private
		 * @param {Array} array The array to iterate over.
		 * @param {Function} callback The function that gets called for every array
		 * item.
		 * @returns {Array} A new array of values returned by the callback function.
		 */
		function map(array, fn) {
			var length = array.length;
			var result = [];
			while (length--) {
				result[length] = fn(array[length]);
			}
			return result;
		}

		/**
		 * A simple `Array#map`-like wrapper to work with domain name strings or email
		 * addresses.
		 * @private
		 * @param {String} domain The domain name or email address.
		 * @param {Function} callback The function that gets called for every
		 * character.
		 * @returns {Array} A new string of characters returned by the callback
		 * function.
		 */
		function mapDomain(string, fn) {
			var parts = string.split('@');
			var result = '';
			if (parts.length > 1) {
				// In email addresses, only the domain name should be punycoded. Leave
				// the local part (i.e. everything up to `@`) intact.
				result = parts[0] + '@';
				string = parts[1];
			}
			// Avoid `split(regex)` for IE8 compatibility. See #17.
			string = string.replace(regexSeparators, '\x2E');
			var labels = string.split('.');
			var encoded = map(labels, fn).join('.');
			return result + encoded;
		}

		/**
		 * Creates an array containing the numeric code points of each Unicode
		 * character in the string. While JavaScript uses UCS-2 internally,
		 * this function will convert a pair of surrogate halves (each of which
		 * UCS-2 exposes as separate characters) into a single code point,
		 * matching UTF-16.
		 * @see `punycode.ucs2.encode`
		 * @see <https://mathiasbynens.be/notes/javascript-encoding>
		 * @memberOf punycode.ucs2
		 * @name decode
		 * @param {String} string The Unicode input string (UCS-2).
		 * @returns {Array} The new array of code points.
		 */
		function ucs2decode(string) {
			var output = [],
			    counter = 0,
			    length = string.length,
			    value,
			    extra;
			while (counter < length) {
				value = string.charCodeAt(counter++);
				if (value >= 0xD800 && value <= 0xDBFF && counter < length) {
					// high surrogate, and there is a next character
					extra = string.charCodeAt(counter++);
					if ((extra & 0xFC00) == 0xDC00) { // low surrogate
						output.push(((value & 0x3FF) << 10) + (extra & 0x3FF) + 0x10000);
					} else {
						// unmatched surrogate; only append this code unit, in case the next
						// code unit is the high surrogate of a surrogate pair
						output.push(value);
						counter--;
					}
				} else {
					output.push(value);
				}
			}
			return output;
		}

		/**
		 * Creates a string based on an array of numeric code points.
		 * @see `punycode.ucs2.decode`
		 * @memberOf punycode.ucs2
		 * @name encode
		 * @param {Array} codePoints The array of numeric code points.
		 * @returns {String} The new Unicode string (UCS-2).
		 */
		function ucs2encode(array) {
			return map(array, function(value) {
				var output = '';
				if (value > 0xFFFF) {
					value -= 0x10000;
					output += stringFromCharCode(value >>> 10 & 0x3FF | 0xD800);
					value = 0xDC00 | value & 0x3FF;
				}
				output += stringFromCharCode(value);
				return output;
			}).join('');
		}

		/**
		 * Converts a basic code point into a digit/integer.
		 * @see `digitToBasic()`
		 * @private
		 * @param {Number} codePoint The basic numeric code point value.
		 * @returns {Number} The numeric value of a basic code point (for use in
		 * representing integers) in the range `0` to `base - 1`, or `base` if
		 * the code point does not represent a value.
		 */
		function basicToDigit(codePoint) {
			if (codePoint - 48 < 10) {
				return codePoint - 22;
			}
			if (codePoint - 65 < 26) {
				return codePoint - 65;
			}
			if (codePoint - 97 < 26) {
				return codePoint - 97;
			}
			return base;
		}

		/**
		 * Converts a digit/integer into a basic code point.
		 * @see `basicToDigit()`
		 * @private
		 * @param {Number} digit The numeric value of a basic code point.
		 * @returns {Number} The basic code point whose value (when used for
		 * representing integers) is `digit`, which needs to be in the range
		 * `0` to `base - 1`. If `flag` is non-zero, the uppercase form is
		 * used; else, the lowercase form is used. The behavior is undefined
		 * if `flag` is non-zero and `digit` has no uppercase form.
		 */
		function digitToBasic(digit, flag) {
			//  0..25 map to ASCII a..z or A..Z
			// 26..35 map to ASCII 0..9
			return digit + 22 + 75 * (digit < 26) - ((flag != 0) << 5);
		}

		/**
		 * Bias adaptation function as per section 3.4 of RFC 3492.
		 * http://tools.ietf.org/html/rfc3492#section-3.4
		 * @private
		 */
		function adapt(delta, numPoints, firstTime) {
			var k = 0;
			delta = firstTime ? floor(delta / damp) : delta >> 1;
			delta += floor(delta / numPoints);
			for (/* no initialization */; delta > baseMinusTMin * tMax >> 1; k += base) {
				delta = floor(delta / baseMinusTMin);
			}
			return floor(k + (baseMinusTMin + 1) * delta / (delta + skew));
		}

		/**
		 * Converts a Punycode string of ASCII-only symbols to a string of Unicode
		 * symbols.
		 * @memberOf punycode
		 * @param {String} input The Punycode string of ASCII-only symbols.
		 * @returns {String} The resulting string of Unicode symbols.
		 */
		function decode(input) {
			// Don't use UCS-2
			var output = [],
			    inputLength = input.length,
			    out,
			    i = 0,
			    n = initialN,
			    bias = initialBias,
			    basic,
			    j,
			    index,
			    oldi,
			    w,
			    k,
			    digit,
			    t,
			    /** Cached calculation results */
			    baseMinusT;

			// Handle the basic code points: let `basic` be the number of input code
			// points before the last delimiter, or `0` if there is none, then copy
			// the first basic code points to the output.

			basic = input.lastIndexOf(delimiter);
			if (basic < 0) {
				basic = 0;
			}

			for (j = 0; j < basic; ++j) {
				// if it's not a basic code point
				if (input.charCodeAt(j) >= 0x80) {
					error('not-basic');
				}
				output.push(input.charCodeAt(j));
			}

			// Main decoding loop: start just after the last delimiter if any basic code
			// points were copied; start at the beginning otherwise.

			for (index = basic > 0 ? basic + 1 : 0; index < inputLength; /* no final expression */) {

				// `index` is the index of the next character to be consumed.
				// Decode a generalized variable-length integer into `delta`,
				// which gets added to `i`. The overflow checking is easier
				// if we increase `i` as we go, then subtract off its starting
				// value at the end to obtain `delta`.
				for (oldi = i, w = 1, k = base; /* no condition */; k += base) {

					if (index >= inputLength) {
						error('invalid-input');
					}

					digit = basicToDigit(input.charCodeAt(index++));

					if (digit >= base || digit > floor((maxInt - i) / w)) {
						error('overflow');
					}

					i += digit * w;
					t = k <= bias ? tMin : (k >= bias + tMax ? tMax : k - bias);

					if (digit < t) {
						break;
					}

					baseMinusT = base - t;
					if (w > floor(maxInt / baseMinusT)) {
						error('overflow');
					}

					w *= baseMinusT;

				}

				out = output.length + 1;
				bias = adapt(i - oldi, out, oldi == 0);

				// `i` was supposed to wrap around from `out` to `0`,
				// incrementing `n` each time, so we'll fix that now:
				if (floor(i / out) > maxInt - n) {
					error('overflow');
				}

				n += floor(i / out);
				i %= out;

				// Insert `n` at position `i` of the output
				output.splice(i++, 0, n);

			}

			return ucs2encode(output);
		}

		/**
		 * Converts a string of Unicode symbols (e.g. a domain name label) to a
		 * Punycode string of ASCII-only symbols.
		 * @memberOf punycode
		 * @param {String} input The string of Unicode symbols.
		 * @returns {String} The resulting Punycode string of ASCII-only symbols.
		 */
		function encode(input) {
			var n,
			    delta,
			    handledCPCount,
			    basicLength,
			    bias,
			    j,
			    m,
			    q,
			    k,
			    t,
			    currentValue,
			    output = [],
			    /** `inputLength` will hold the number of code points in `input`. */
			    inputLength,
			    /** Cached calculation results */
			    handledCPCountPlusOne,
			    baseMinusT,
			    qMinusT;

			// Convert the input in UCS-2 to Unicode
			input = ucs2decode(input);

			// Cache the length
			inputLength = input.length;

			// Initialize the state
			n = initialN;
			delta = 0;
			bias = initialBias;

			// Handle the basic code points
			for (j = 0; j < inputLength; ++j) {
				currentValue = input[j];
				if (currentValue < 0x80) {
					output.push(stringFromCharCode(currentValue));
				}
			}

			handledCPCount = basicLength = output.length;

			// `handledCPCount` is the number of code points that have been handled;
			// `basicLength` is the number of basic code points.

			// Finish the basic string - if it is not empty - with a delimiter
			if (basicLength) {
				output.push(delimiter);
			}

			// Main encoding loop:
			while (handledCPCount < inputLength) {

				// All non-basic code points < n have been handled already. Find the next
				// larger one:
				for (m = maxInt, j = 0; j < inputLength; ++j) {
					currentValue = input[j];
					if (currentValue >= n && currentValue < m) {
						m = currentValue;
					}
				}

				// Increase `delta` enough to advance the decoder's <n,i> state to <m,0>,
				// but guard against overflow
				handledCPCountPlusOne = handledCPCount + 1;
				if (m - n > floor((maxInt - delta) / handledCPCountPlusOne)) {
					error('overflow');
				}

				delta += (m - n) * handledCPCountPlusOne;
				n = m;

				for (j = 0; j < inputLength; ++j) {
					currentValue = input[j];

					if (currentValue < n && ++delta > maxInt) {
						error('overflow');
					}

					if (currentValue == n) {
						// Represent delta as a generalized variable-length integer
						for (q = delta, k = base; /* no condition */; k += base) {
							t = k <= bias ? tMin : (k >= bias + tMax ? tMax : k - bias);
							if (q < t) {
								break;
							}
							qMinusT = q - t;
							baseMinusT = base - t;
							output.push(
								stringFromCharCode(digitToBasic(t + qMinusT % baseMinusT, 0))
							);
							q = floor(qMinusT / baseMinusT);
						}

						output.push(stringFromCharCode(digitToBasic(q, 0)));
						bias = adapt(delta, handledCPCountPlusOne, handledCPCount == basicLength);
						delta = 0;
						++handledCPCount;
					}
				}

				++delta;
				++n;

			}
			return output.join('');
		}

		/**
		 * Converts a Punycode string representing a domain name or an email address
		 * to Unicode. Only the Punycoded parts of the input will be converted, i.e.
		 * it doesn't matter if you call it on a string that has already been
		 * converted to Unicode.
		 * @memberOf punycode
		 * @param {String} input The Punycoded domain name or email address to
		 * convert to Unicode.
		 * @returns {String} The Unicode representation of the given Punycode
		 * string.
		 */
		function toUnicode(input) {
			return mapDomain(input, function(string) {
				return regexPunycode.test(string)
					? decode(string.slice(4).toLowerCase())
					: string;
			});
		}

		/**
		 * Converts a Unicode string representing a domain name or an email address to
		 * Punycode. Only the non-ASCII parts of the domain name will be converted,
		 * i.e. it doesn't matter if you call it with a domain that's already in
		 * ASCII.
		 * @memberOf punycode
		 * @param {String} input The domain name or email address to convert, as a
		 * Unicode string.
		 * @returns {String} The Punycode representation of the given domain name or
		 * email address.
		 */
		function toASCII(input) {
			return mapDomain(input, function(string) {
				return regexNonASCII.test(string)
					? 'xn--' + encode(string)
					: string;
			});
		}

		/*--------------------------------------------------------------------------*/

		/** Define the public API */
		punycode = {
			/**
			 * A string representing the current Punycode.js version number.
			 * @memberOf punycode
			 * @type String
			 */
			'version': '1.3.2',
			/**
			 * An object of methods to convert from JavaScript's internal character
			 * representation (UCS-2) to Unicode code points, and back.
			 * @see <https://mathiasbynens.be/notes/javascript-encoding>
			 * @memberOf punycode
			 * @type Object
			 */
			'ucs2': {
				'decode': ucs2decode,
				'encode': ucs2encode
			},
			'decode': decode,
			'encode': encode,
			'toASCII': toASCII,
			'toUnicode': toUnicode
		};

		/** Expose `punycode` */
		// Some AMD build optimizers, like r.js, check for specific condition patterns
		// like the following:
		if (
			true
		) {
			!(__WEBPACK_AMD_DEFINE_RESULT__ = function() {
				return punycode;
			}.call(exports, __webpack_require__, exports, module), __WEBPACK_AMD_DEFINE_RESULT__ !== undefined && (module.exports = __WEBPACK_AMD_DEFINE_RESULT__));
		} else if (freeExports && freeModule) {
			if (module.exports == freeExports) { // in Node.js or RingoJS v0.8.0+
				freeModule.exports = punycode;
			} else { // in Narwhal or RingoJS v0.7.0-
				for (key in punycode) {
					punycode.hasOwnProperty(key) && (freeExports[key] = punycode[key]);
				}
			}
		} else { // in Rhino or a web browser
			root.punycode = punycode;
		}

	}(this));

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(96)(module), (function() { return this; }())))

/***/ }),
/* 96 */
/***/ (function(module, exports) {

	module.exports = function(module) {
		if(!module.webpackPolyfill) {
			module.deprecate = function() {};
			module.paths = [];
			// module.parent = undefined by default
			module.children = [];
			module.webpackPolyfill = 1;
		}
		return module;
	}


/***/ }),
/* 97 */
/***/ (function(module, exports) {

	'use strict';

	module.exports = {
	  isString: function(arg) {
	    return typeof(arg) === 'string';
	  },
	  isObject: function(arg) {
	    return typeof(arg) === 'object' && arg !== null;
	  },
	  isNull: function(arg) {
	    return arg === null;
	  },
	  isNullOrUndefined: function(arg) {
	    return arg == null;
	  }
	};


/***/ }),
/* 98 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	exports.decode = exports.parse = __webpack_require__(99);
	exports.encode = exports.stringify = __webpack_require__(100);


/***/ }),
/* 99 */
/***/ (function(module, exports) {

	// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	'use strict';

	// If obj.hasOwnProperty has been overridden, then calling
	// obj.hasOwnProperty(prop) will break.
	// See: https://github.com/joyent/node/issues/1707
	function hasOwnProperty(obj, prop) {
	  return Object.prototype.hasOwnProperty.call(obj, prop);
	}

	module.exports = function(qs, sep, eq, options) {
	  sep = sep || '&';
	  eq = eq || '=';
	  var obj = {};

	  if (typeof qs !== 'string' || qs.length === 0) {
	    return obj;
	  }

	  var regexp = /\+/g;
	  qs = qs.split(sep);

	  var maxKeys = 1000;
	  if (options && typeof options.maxKeys === 'number') {
	    maxKeys = options.maxKeys;
	  }

	  var len = qs.length;
	  // maxKeys <= 0 means that we should not limit keys count
	  if (maxKeys > 0 && len > maxKeys) {
	    len = maxKeys;
	  }

	  for (var i = 0; i < len; ++i) {
	    var x = qs[i].replace(regexp, '%20'),
	        idx = x.indexOf(eq),
	        kstr, vstr, k, v;

	    if (idx >= 0) {
	      kstr = x.substr(0, idx);
	      vstr = x.substr(idx + 1);
	    } else {
	      kstr = x;
	      vstr = '';
	    }

	    k = decodeURIComponent(kstr);
	    v = decodeURIComponent(vstr);

	    if (!hasOwnProperty(obj, k)) {
	      obj[k] = v;
	    } else if (Array.isArray(obj[k])) {
	      obj[k].push(v);
	    } else {
	      obj[k] = [obj[k], v];
	    }
	  }

	  return obj;
	};


/***/ }),
/* 100 */
/***/ (function(module, exports) {

	// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	'use strict';

	var stringifyPrimitive = function(v) {
	  switch (typeof v) {
	    case 'string':
	      return v;

	    case 'boolean':
	      return v ? 'true' : 'false';

	    case 'number':
	      return isFinite(v) ? v : '';

	    default:
	      return '';
	  }
	};

	module.exports = function(obj, sep, eq, name) {
	  sep = sep || '&';
	  eq = eq || '=';
	  if (obj === null) {
	    obj = undefined;
	  }

	  if (typeof obj === 'object') {
	    return Object.keys(obj).map(function(k) {
	      var ks = encodeURIComponent(stringifyPrimitive(k)) + eq;
	      if (Array.isArray(obj[k])) {
	        return obj[k].map(function(v) {
	          return ks + encodeURIComponent(stringifyPrimitive(v));
	        }).join(sep);
	      } else {
	        return ks + encodeURIComponent(stringifyPrimitive(obj[k]));
	      }
	    }).join(sep);

	  }

	  if (!name) return '';
	  return encodeURIComponent(stringifyPrimitive(name)) + eq +
	         encodeURIComponent(stringifyPrimitive(obj));
	};


/***/ }),
/* 101 */
/***/ (function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__(102);


/***/ }),
/* 102 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer, process) {'use strict';

	var net = __webpack_require__(!(function webpackMissingModule() { var e = new Error("Cannot find module \"net\""); e.code = 'MODULE_NOT_FOUND'; throw e; }()));
	var tls = __webpack_require__(!(function webpackMissingModule() { var e = new Error("Cannot find module \"tls\""); e.code = 'MODULE_NOT_FOUND'; throw e; }()));
	var http = __webpack_require__(74);
	var https = __webpack_require__(73);
	var events = __webpack_require__(34);
	var assert = __webpack_require__(103);
	var util = __webpack_require__(17);


	exports.httpOverHttp = httpOverHttp;
	exports.httpsOverHttp = httpsOverHttp;
	exports.httpOverHttps = httpOverHttps;
	exports.httpsOverHttps = httpsOverHttps;


	function httpOverHttp(options) {
	  var agent = new TunnelingAgent(options);
	  agent.request = http.request;
	  return agent;
	}

	function httpsOverHttp(options) {
	  var agent = new TunnelingAgent(options);
	  agent.request = http.request;
	  agent.createSocket = createSecureSocket;
	  return agent;
	}

	function httpOverHttps(options) {
	  var agent = new TunnelingAgent(options);
	  agent.request = https.request;
	  return agent;
	}

	function httpsOverHttps(options) {
	  var agent = new TunnelingAgent(options);
	  agent.request = https.request;
	  agent.createSocket = createSecureSocket;
	  return agent;
	}


	function TunnelingAgent(options) {
	  var self = this;
	  self.options = options || {};
	  self.proxyOptions = self.options.proxy || {};
	  self.maxSockets = self.options.maxSockets || http.Agent.defaultMaxSockets;
	  self.requests = [];
	  self.sockets = [];

	  self.on('free', function onFree(socket, host, port, localAddress) {
	    var options = toOptions(host, port, localAddress);
	    for (var i = 0, len = self.requests.length; i < len; ++i) {
	      var pending = self.requests[i];
	      if (pending.host === options.host && pending.port === options.port) {
	        // Detect the request to connect same origin server,
	        // reuse the connection.
	        self.requests.splice(i, 1);
	        pending.request.onSocket(socket);
	        return;
	      }
	    }
	    socket.destroy();
	    self.removeSocket(socket);
	  });
	}
	util.inherits(TunnelingAgent, events.EventEmitter);

	TunnelingAgent.prototype.addRequest = function addRequest(req, host, port, localAddress) {
	  var self = this;
	  var options = mergeOptions({request: req}, self.options, toOptions(host, port, localAddress));

	  if (self.sockets.length >= this.maxSockets) {
	    // We are over limit so we'll add it to the queue.
	    self.requests.push(options);
	    return;
	  }

	  // If we are under maxSockets create a new one.
	  self.createSocket(options, function(socket) {
	    socket.on('free', onFree);
	    socket.on('close', onCloseOrRemove);
	    socket.on('agentRemove', onCloseOrRemove);
	    req.onSocket(socket);

	    function onFree() {
	      self.emit('free', socket, options);
	    }

	    function onCloseOrRemove(err) {
	      self.removeSocket(socket);
	      socket.removeListener('free', onFree);
	      socket.removeListener('close', onCloseOrRemove);
	      socket.removeListener('agentRemove', onCloseOrRemove);
	    }
	  });
	};

	TunnelingAgent.prototype.createSocket = function createSocket(options, cb) {
	  var self = this;
	  var placeholder = {};
	  self.sockets.push(placeholder);

	  var connectOptions = mergeOptions({}, self.proxyOptions, {
	    method: 'CONNECT',
	    path: options.host + ':' + options.port,
	    agent: false
	  });
	  if (connectOptions.proxyAuth) {
	    connectOptions.headers = connectOptions.headers || {};
	    connectOptions.headers['Proxy-Authorization'] = 'Basic ' +
	        new Buffer(connectOptions.proxyAuth).toString('base64');
	  }

	  debug('making CONNECT request');
	  var connectReq = self.request(connectOptions);
	  connectReq.useChunkedEncodingByDefault = false; // for v0.6
	  connectReq.once('response', onResponse); // for v0.6
	  connectReq.once('upgrade', onUpgrade);   // for v0.6
	  connectReq.once('connect', onConnect);   // for v0.7 or later
	  connectReq.once('error', onError);
	  connectReq.end();

	  function onResponse(res) {
	    // Very hacky. This is necessary to avoid http-parser leaks.
	    res.upgrade = true;
	  }

	  function onUpgrade(res, socket, head) {
	    // Hacky.
	    process.nextTick(function() {
	      onConnect(res, socket, head);
	    });
	  }

	  function onConnect(res, socket, head) {
	    connectReq.removeAllListeners();
	    socket.removeAllListeners();

	    if (res.statusCode === 200) {
	      assert.equal(head.length, 0);
	      debug('tunneling connection has established');
	      self.sockets[self.sockets.indexOf(placeholder)] = socket;
	      cb(socket);
	    } else {
	      debug('tunneling socket could not be established, statusCode=%d',
	            res.statusCode);
	      socket.destroy();
	      var error = new Error('tunneling socket could not be established, ' +
	                            'statusCode=' + res.statusCode);
	      error.code = 'ECONNRESET';
	      options.request.emit('error', error);
	      self.removeSocket(placeholder);
	    }
	  }

	  function onError(cause) {
	    connectReq.removeAllListeners();

	    debug('tunneling socket could not be established, cause=%s\n',
	          cause.message, cause.stack);
	    var error = new Error('tunneling socket could not be established, ' +
	                          'cause=' + cause.message);
	    error.code = 'ECONNRESET';
	    options.request.emit('error', error);
	    self.removeSocket(placeholder);
	  }
	};

	TunnelingAgent.prototype.removeSocket = function removeSocket(socket) {
	  var pos = this.sockets.indexOf(socket)
	  if (pos === -1) {
	    return;
	  }
	  this.sockets.splice(pos, 1);

	  var pending = this.requests.shift();
	  if (pending) {
	    // If we have pending requests and a socket gets closed a new one
	    // needs to be created to take over in the pool for the one that closed.
	    this.createSocket(pending, function(socket) {
	      pending.request.onSocket(socket);
	    });
	  }
	};

	function createSecureSocket(options, cb) {
	  var self = this;
	  TunnelingAgent.prototype.createSocket.call(self, options, function(socket) {
	    var hostHeader = options.request.getHeader('host');
	    var tlsOptions = mergeOptions({}, self.options, {
	      socket: socket,
	      servername: hostHeader ? hostHeader.replace(/:.*$/, '') : options.host
	    });

	    // 0 is dummy port for v0.6
	    var secureSocket = tls.connect(0, tlsOptions);
	    self.sockets[self.sockets.indexOf(socket)] = secureSocket;
	    cb(secureSocket);
	  });
	}


	function toOptions(host, port, localAddress) {
	  if (typeof host === 'string') { // since v0.10
	    return {
	      host: host,
	      port: port,
	      localAddress: localAddress
	    };
	  }
	  return host; // for v0.11 or later
	}

	function mergeOptions(target) {
	  for (var i = 1, len = arguments.length; i < len; ++i) {
	    var overrides = arguments[i];
	    if (typeof overrides === 'object') {
	      var keys = Object.keys(overrides);
	      for (var j = 0, keyLen = keys.length; j < keyLen; ++j) {
	        var k = keys[j];
	        if (overrides[k] !== undefined) {
	          target[k] = overrides[k];
	        }
	      }
	    }
	  }
	  return target;
	}


	var debug;
	if (process.env.NODE_DEBUG && /\btunnel\b/.test(process.env.NODE_DEBUG)) {
	  debug = function() {
	    var args = Array.prototype.slice.call(arguments);
	    if (typeof args[0] === 'string') {
	      args[0] = 'TUNNEL: ' + args[0];
	    } else {
	      args.unshift('TUNNEL:');
	    }
	    console.error.apply(console, args);
	  }
	} else {
	  debug = function() {};
	}
	exports.debug = debug; // for test

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer, __webpack_require__(18)))

/***/ }),
/* 103 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(global) {'use strict';

	// compare and isBuffer taken from https://github.com/feross/buffer/blob/680e9e5e488f22aac27599a57dc844a6315928dd/index.js
	// original notice:

	/*!
	 * The buffer module from node.js, for the browser.
	 *
	 * @author   Feross Aboukhadijeh <feross@feross.org> <http://feross.org>
	 * @license  MIT
	 */
	function compare(a, b) {
	  if (a === b) {
	    return 0;
	  }

	  var x = a.length;
	  var y = b.length;

	  for (var i = 0, len = Math.min(x, y); i < len; ++i) {
	    if (a[i] !== b[i]) {
	      x = a[i];
	      y = b[i];
	      break;
	    }
	  }

	  if (x < y) {
	    return -1;
	  }
	  if (y < x) {
	    return 1;
	  }
	  return 0;
	}
	function isBuffer(b) {
	  if (global.Buffer && typeof global.Buffer.isBuffer === 'function') {
	    return global.Buffer.isBuffer(b);
	  }
	  return !!(b != null && b._isBuffer);
	}

	// based on node assert, original notice:

	// http://wiki.commonjs.org/wiki/Unit_Testing/1.0
	//
	// THIS IS NOT TESTED NOR LIKELY TO WORK OUTSIDE V8!
	//
	// Originally from narwhal.js (http://narwhaljs.org)
	// Copyright (c) 2009 Thomas Robinson <280north.com>
	//
	// Permission is hereby granted, free of charge, to any person obtaining a copy
	// of this software and associated documentation files (the 'Software'), to
	// deal in the Software without restriction, including without limitation the
	// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
	// sell copies of the Software, and to permit persons to whom the Software is
	// furnished to do so, subject to the following conditions:
	//
	// The above copyright notice and this permission notice shall be included in
	// all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	// AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
	// ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
	// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

	var util = __webpack_require__(17);
	var hasOwn = Object.prototype.hasOwnProperty;
	var pSlice = Array.prototype.slice;
	var functionsHaveNames = (function () {
	  return function foo() {}.name === 'foo';
	}());
	function pToString (obj) {
	  return Object.prototype.toString.call(obj);
	}
	function isView(arrbuf) {
	  if (isBuffer(arrbuf)) {
	    return false;
	  }
	  if (typeof global.ArrayBuffer !== 'function') {
	    return false;
	  }
	  if (typeof ArrayBuffer.isView === 'function') {
	    return ArrayBuffer.isView(arrbuf);
	  }
	  if (!arrbuf) {
	    return false;
	  }
	  if (arrbuf instanceof DataView) {
	    return true;
	  }
	  if (arrbuf.buffer && arrbuf.buffer instanceof ArrayBuffer) {
	    return true;
	  }
	  return false;
	}
	// 1. The assert module provides functions that throw
	// AssertionError's when particular conditions are not met. The
	// assert module must conform to the following interface.

	var assert = module.exports = ok;

	// 2. The AssertionError is defined in assert.
	// new assert.AssertionError({ message: message,
	//                             actual: actual,
	//                             expected: expected })

	var regex = /\s*function\s+([^\(\s]*)\s*/;
	// based on https://github.com/ljharb/function.prototype.name/blob/adeeeec8bfcc6068b187d7d9fb3d5bb1d3a30899/implementation.js
	function getName(func) {
	  if (!util.isFunction(func)) {
	    return;
	  }
	  if (functionsHaveNames) {
	    return func.name;
	  }
	  var str = func.toString();
	  var match = str.match(regex);
	  return match && match[1];
	}
	assert.AssertionError = function AssertionError(options) {
	  this.name = 'AssertionError';
	  this.actual = options.actual;
	  this.expected = options.expected;
	  this.operator = options.operator;
	  if (options.message) {
	    this.message = options.message;
	    this.generatedMessage = false;
	  } else {
	    this.message = getMessage(this);
	    this.generatedMessage = true;
	  }
	  var stackStartFunction = options.stackStartFunction || fail;
	  if (Error.captureStackTrace) {
	    Error.captureStackTrace(this, stackStartFunction);
	  } else {
	    // non v8 browsers so we can have a stacktrace
	    var err = new Error();
	    if (err.stack) {
	      var out = err.stack;

	      // try to strip useless frames
	      var fn_name = getName(stackStartFunction);
	      var idx = out.indexOf('\n' + fn_name);
	      if (idx >= 0) {
	        // once we have located the function frame
	        // we need to strip out everything before it (and its line)
	        var next_line = out.indexOf('\n', idx + 1);
	        out = out.substring(next_line + 1);
	      }

	      this.stack = out;
	    }
	  }
	};

	// assert.AssertionError instanceof Error
	util.inherits(assert.AssertionError, Error);

	function truncate(s, n) {
	  if (typeof s === 'string') {
	    return s.length < n ? s : s.slice(0, n);
	  } else {
	    return s;
	  }
	}
	function inspect(something) {
	  if (functionsHaveNames || !util.isFunction(something)) {
	    return util.inspect(something);
	  }
	  var rawname = getName(something);
	  var name = rawname ? ': ' + rawname : '';
	  return '[Function' +  name + ']';
	}
	function getMessage(self) {
	  return truncate(inspect(self.actual), 128) + ' ' +
	         self.operator + ' ' +
	         truncate(inspect(self.expected), 128);
	}

	// At present only the three keys mentioned above are used and
	// understood by the spec. Implementations or sub modules can pass
	// other keys to the AssertionError's constructor - they will be
	// ignored.

	// 3. All of the following functions must throw an AssertionError
	// when a corresponding condition is not met, with a message that
	// may be undefined if not provided.  All assertion methods provide
	// both the actual and expected values to the assertion error for
	// display purposes.

	function fail(actual, expected, message, operator, stackStartFunction) {
	  throw new assert.AssertionError({
	    message: message,
	    actual: actual,
	    expected: expected,
	    operator: operator,
	    stackStartFunction: stackStartFunction
	  });
	}

	// EXTENSION! allows for well behaved errors defined elsewhere.
	assert.fail = fail;

	// 4. Pure assertion tests whether a value is truthy, as determined
	// by !!guard.
	// assert.ok(guard, message_opt);
	// This statement is equivalent to assert.equal(true, !!guard,
	// message_opt);. To test strictly for the value true, use
	// assert.strictEqual(true, guard, message_opt);.

	function ok(value, message) {
	  if (!value) fail(value, true, message, '==', assert.ok);
	}
	assert.ok = ok;

	// 5. The equality assertion tests shallow, coercive equality with
	// ==.
	// assert.equal(actual, expected, message_opt);

	assert.equal = function equal(actual, expected, message) {
	  if (actual != expected) fail(actual, expected, message, '==', assert.equal);
	};

	// 6. The non-equality assertion tests for whether two objects are not equal
	// with != assert.notEqual(actual, expected, message_opt);

	assert.notEqual = function notEqual(actual, expected, message) {
	  if (actual == expected) {
	    fail(actual, expected, message, '!=', assert.notEqual);
	  }
	};

	// 7. The equivalence assertion tests a deep equality relation.
	// assert.deepEqual(actual, expected, message_opt);

	assert.deepEqual = function deepEqual(actual, expected, message) {
	  if (!_deepEqual(actual, expected, false)) {
	    fail(actual, expected, message, 'deepEqual', assert.deepEqual);
	  }
	};

	assert.deepStrictEqual = function deepStrictEqual(actual, expected, message) {
	  if (!_deepEqual(actual, expected, true)) {
	    fail(actual, expected, message, 'deepStrictEqual', assert.deepStrictEqual);
	  }
	};

	function _deepEqual(actual, expected, strict, memos) {
	  // 7.1. All identical values are equivalent, as determined by ===.
	  if (actual === expected) {
	    return true;
	  } else if (isBuffer(actual) && isBuffer(expected)) {
	    return compare(actual, expected) === 0;

	  // 7.2. If the expected value is a Date object, the actual value is
	  // equivalent if it is also a Date object that refers to the same time.
	  } else if (util.isDate(actual) && util.isDate(expected)) {
	    return actual.getTime() === expected.getTime();

	  // 7.3 If the expected value is a RegExp object, the actual value is
	  // equivalent if it is also a RegExp object with the same source and
	  // properties (`global`, `multiline`, `lastIndex`, `ignoreCase`).
	  } else if (util.isRegExp(actual) && util.isRegExp(expected)) {
	    return actual.source === expected.source &&
	           actual.global === expected.global &&
	           actual.multiline === expected.multiline &&
	           actual.lastIndex === expected.lastIndex &&
	           actual.ignoreCase === expected.ignoreCase;

	  // 7.4. Other pairs that do not both pass typeof value == 'object',
	  // equivalence is determined by ==.
	  } else if ((actual === null || typeof actual !== 'object') &&
	             (expected === null || typeof expected !== 'object')) {
	    return strict ? actual === expected : actual == expected;

	  // If both values are instances of typed arrays, wrap their underlying
	  // ArrayBuffers in a Buffer each to increase performance
	  // This optimization requires the arrays to have the same type as checked by
	  // Object.prototype.toString (aka pToString). Never perform binary
	  // comparisons for Float*Arrays, though, since e.g. +0 === -0 but their
	  // bit patterns are not identical.
	  } else if (isView(actual) && isView(expected) &&
	             pToString(actual) === pToString(expected) &&
	             !(actual instanceof Float32Array ||
	               actual instanceof Float64Array)) {
	    return compare(new Uint8Array(actual.buffer),
	                   new Uint8Array(expected.buffer)) === 0;

	  // 7.5 For all other Object pairs, including Array objects, equivalence is
	  // determined by having the same number of owned properties (as verified
	  // with Object.prototype.hasOwnProperty.call), the same set of keys
	  // (although not necessarily the same order), equivalent values for every
	  // corresponding key, and an identical 'prototype' property. Note: this
	  // accounts for both named and indexed properties on Arrays.
	  } else if (isBuffer(actual) !== isBuffer(expected)) {
	    return false;
	  } else {
	    memos = memos || {actual: [], expected: []};

	    var actualIndex = memos.actual.indexOf(actual);
	    if (actualIndex !== -1) {
	      if (actualIndex === memos.expected.indexOf(expected)) {
	        return true;
	      }
	    }

	    memos.actual.push(actual);
	    memos.expected.push(expected);

	    return objEquiv(actual, expected, strict, memos);
	  }
	}

	function isArguments(object) {
	  return Object.prototype.toString.call(object) == '[object Arguments]';
	}

	function objEquiv(a, b, strict, actualVisitedObjects) {
	  if (a === null || a === undefined || b === null || b === undefined)
	    return false;
	  // if one is a primitive, the other must be same
	  if (util.isPrimitive(a) || util.isPrimitive(b))
	    return a === b;
	  if (strict && Object.getPrototypeOf(a) !== Object.getPrototypeOf(b))
	    return false;
	  var aIsArgs = isArguments(a);
	  var bIsArgs = isArguments(b);
	  if ((aIsArgs && !bIsArgs) || (!aIsArgs && bIsArgs))
	    return false;
	  if (aIsArgs) {
	    a = pSlice.call(a);
	    b = pSlice.call(b);
	    return _deepEqual(a, b, strict);
	  }
	  var ka = objectKeys(a);
	  var kb = objectKeys(b);
	  var key, i;
	  // having the same number of owned properties (keys incorporates
	  // hasOwnProperty)
	  if (ka.length !== kb.length)
	    return false;
	  //the same set of keys (although not necessarily the same order),
	  ka.sort();
	  kb.sort();
	  //~~~cheap key test
	  for (i = ka.length - 1; i >= 0; i--) {
	    if (ka[i] !== kb[i])
	      return false;
	  }
	  //equivalent values for every corresponding key, and
	  //~~~possibly expensive deep test
	  for (i = ka.length - 1; i >= 0; i--) {
	    key = ka[i];
	    if (!_deepEqual(a[key], b[key], strict, actualVisitedObjects))
	      return false;
	  }
	  return true;
	}

	// 8. The non-equivalence assertion tests for any deep inequality.
	// assert.notDeepEqual(actual, expected, message_opt);

	assert.notDeepEqual = function notDeepEqual(actual, expected, message) {
	  if (_deepEqual(actual, expected, false)) {
	    fail(actual, expected, message, 'notDeepEqual', assert.notDeepEqual);
	  }
	};

	assert.notDeepStrictEqual = notDeepStrictEqual;
	function notDeepStrictEqual(actual, expected, message) {
	  if (_deepEqual(actual, expected, true)) {
	    fail(actual, expected, message, 'notDeepStrictEqual', notDeepStrictEqual);
	  }
	}


	// 9. The strict equality assertion tests strict equality, as determined by ===.
	// assert.strictEqual(actual, expected, message_opt);

	assert.strictEqual = function strictEqual(actual, expected, message) {
	  if (actual !== expected) {
	    fail(actual, expected, message, '===', assert.strictEqual);
	  }
	};

	// 10. The strict non-equality assertion tests for strict inequality, as
	// determined by !==.  assert.notStrictEqual(actual, expected, message_opt);

	assert.notStrictEqual = function notStrictEqual(actual, expected, message) {
	  if (actual === expected) {
	    fail(actual, expected, message, '!==', assert.notStrictEqual);
	  }
	};

	function expectedException(actual, expected) {
	  if (!actual || !expected) {
	    return false;
	  }

	  if (Object.prototype.toString.call(expected) == '[object RegExp]') {
	    return expected.test(actual);
	  }

	  try {
	    if (actual instanceof expected) {
	      return true;
	    }
	  } catch (e) {
	    // Ignore.  The instanceof check doesn't work for arrow functions.
	  }

	  if (Error.isPrototypeOf(expected)) {
	    return false;
	  }

	  return expected.call({}, actual) === true;
	}

	function _tryBlock(block) {
	  var error;
	  try {
	    block();
	  } catch (e) {
	    error = e;
	  }
	  return error;
	}

	function _throws(shouldThrow, block, expected, message) {
	  var actual;

	  if (typeof block !== 'function') {
	    throw new TypeError('"block" argument must be a function');
	  }

	  if (typeof expected === 'string') {
	    message = expected;
	    expected = null;
	  }

	  actual = _tryBlock(block);

	  message = (expected && expected.name ? ' (' + expected.name + ').' : '.') +
	            (message ? ' ' + message : '.');

	  if (shouldThrow && !actual) {
	    fail(actual, expected, 'Missing expected exception' + message);
	  }

	  var userProvidedMessage = typeof message === 'string';
	  var isUnwantedException = !shouldThrow && util.isError(actual);
	  var isUnexpectedException = !shouldThrow && actual && !expected;

	  if ((isUnwantedException &&
	      userProvidedMessage &&
	      expectedException(actual, expected)) ||
	      isUnexpectedException) {
	    fail(actual, expected, 'Got unwanted exception' + message);
	  }

	  if ((shouldThrow && actual && expected &&
	      !expectedException(actual, expected)) || (!shouldThrow && actual)) {
	    throw actual;
	  }
	}

	// 11. Expected to throw an error:
	// assert.throws(block, Error_opt, message_opt);

	assert.throws = function(block, /*optional*/error, /*optional*/message) {
	  _throws(true, block, error, message);
	};

	// EXTENSION! This is annoying to write outside this module.
	assert.doesNotThrow = function(block, /*optional*/error, /*optional*/message) {
	  _throws(false, block, error, message);
	};

	assert.ifError = function(err) { if (err) throw err; };

	var objectKeys = Object.keys || function (obj) {
	  var keys = [];
	  for (var key in obj) {
	    if (hasOwn.call(obj, key)) keys.push(key);
	  }
	  return keys;
	};

	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }())))

/***/ }),
/* 104 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4),
	    RetryOptions = __webpack_require__(105);
	//SCRIPT START

	var AzureDocuments = Base.defineClass(null, null,
	    {
	       /**
	         * Represents a DatabaseAccount in the Azure Cosmos DB database service. A DatabaseAccount is the container for databases.
	         * @global
	         * @property {string} DatabasesLink                                     -  The self-link for Databases in the databaseAccount.
	         * @property {string} MediaLink                                         -  The self-link for Media in the databaseAccount.
	         * @property {number} MaxMediaStorageUsageInMB                          -  Attachment content (media) storage quota in MBs ( Retrieved from gateway ).
	         * @property {number} CurrentMediaStorageUsageInMB                      -  <p> Current attachment content (media) usage in MBs (Retrieved from gateway )<br>
	                                                                                    Value is returned from cached information updated periodically and is not guaranteed to be real time. </p>
	         * @property {object} ConsistencyPolicy                                 -  Gets the UserConsistencyPolicy settings.
	         * @property {string} ConsistencyPolicy.defaultConsistencyLevel         -  The default consistency level and it's of type {@link ConsistencyLevel}.
	         * @property {number} ConsistencyPolicy.maxStalenessPrefix              -  In bounded staleness consistency, the maximum allowed staleness in terms difference in sequence numbers (aka version).
	         * @property {number} ConsistencyPolicy.maxStalenessIntervalInSeconds   -  In bounded staleness consistency, the maximum allowed staleness in terms time interval.
	         
	         * @property {Array}  WritableLocations                                 -  The list of writable locations for a geo-replicated database account.
	         * @property {Array}  ReadableLocations                                 -  The list of readable locations for a geo-replicated database account.
	         */
	        DatabaseAccount: Base.defineClass(function () {
	            this._writableLocations = [];
	            this._readableLocations = [];

	            Object.defineProperty(this, "DatabasesLink", {
	                value: "",
	                writable: true,
	                configurable: true,
	                enumerable: true
	            });

	            Object.defineProperty(this, "MediaLink", {
	                value: "",
	                writable: true,
	                configurable: true,
	                enumerable: true
	            });

	            Object.defineProperty(this, "MaxMediaStorageUsageInMB", {
	                value: 0,
	                writable: true,
	                configurable: true,
	                enumerable: true
	            });

	            Object.defineProperty(this, "CurrentMediaStorageUsageInMB", {
	                value: 0,
	                writable: true,
	                configurable: true,
	                enumerable: true
	            });

	            Object.defineProperty(this, "ConsumedDocumentStorageInMB", {
	                value: 0,
	                writable: true,
	                configurable: true,
	                enumerable: true
	            });

	            Object.defineProperty(this, "ReservedDocumentStorageInMB", {
	                value: 0,
	                writable: true,
	                configurable: true,
	                enumerable: true
	            });

	            Object.defineProperty(this, "ProvisionedDocumentStorageInMB", {
	                value: 0,
	                writable: true,
	                configurable: true,
	                enumerable: true
	            });

	            Object.defineProperty(this, "ConsistencyPolicy", {
	                value: "",
	                writable: true,
	                configurable: true,
	                enumerable: true
	            });
	        
	            Object.defineProperty(this, "WritableLocations", {
	                get: function () {
	                    return this._writableLocations;
	                },
	                enumerable: true
	            });

	            Object.defineProperty(this, "ReadableLocations", {
	                get: function () {
	                    return this._readableLocations;
	                },
	                enumerable: true
	            });
	        }),

	        /**
	         * <p>Represents the consistency levels supported for Azure Cosmos DB client operations.<br>
	         * The requested ConsistencyLevel must match or be weaker than that provisioned for the database account. Consistency levels.<br>
	         * Consistency levels by order of strength are Strong, BoundedStaleness, Session and Eventual.</p>
	         * @readonly
	         * @enum {string}
	         * @property Strong           Strong Consistency guarantees that read operations always return the value that was last written.
	         * @property BoundedStaleness Bounded Staleness guarantees that reads are not too out-of-date. This can be configured based on number of operations (MaxStalenessPrefix) or time (MaxStalenessIntervalInSeconds).
	         * @property Session          Session Consistency guarantees monotonic reads (you never read old data, then new, then old again), monotonic writes (writes are ordered)
	                                      and read your writes (your writes are immediately visible to your reads) within any single session.
	         * @property Eventual         Eventual Consistency guarantees that reads will return a subset of writes. All writes
	                                      will be eventually be available for reads.
	         * @property ConsistentPrefix ConsistentPrefix Consistency guarantees that reads will return some prefix of all writes with no gaps.
	                                      All writes will be eventually be available for reads.                          
	         */
	        ConsistencyLevel: Object.freeze({
	            Strong: "Strong",
	            BoundedStaleness: "BoundedStaleness",
	            Session: "Session",
	            Eventual: "Eventual",
	            ConsistentPrefix: "ConsistentPrefix"
	        }),


	        /**
	         * Specifies the supported indexing modes.
	         * @readonly
	         * @enum {string}
	         * @property Consistent     <p>Index is updated synchronously with a create or update operation. <br>
	                                    With consistent indexing, query behavior is the same as the default consistency level for the collection. The index is
	                                    always kept up to date with the data. </p>
	         * @property Lazy           <p>Index is updated asynchronously with respect to a create or update operation. <br>
	                                    With lazy indexing, queries are eventually consistent. The index is updated when the collection is idle.</p>
	         */
	        IndexingMode: Object.freeze({
	            Consistent: "consistent",
	            Lazy: "lazy",
	            None: "none"
	        }),

	        /**
	         * Specifies the supported Index types.
	         * @readonly
	         * @enum {string}
	         * @property Hash     This is supplied for a path which has no sorting requirement.
	         *                    This kind of an index has better precision than corresponding range index.
	         * @property Range    This is supplied for a path which requires sorting.
	         * @property Spatial  This is supplied for a path which requires geospatial indexing.
	         */

	        IndexKind: Object.freeze({
	            Hash: "Hash",
	            Range: "Range",
	            Spatial: "Spatial"
	        }),

	        DataType: Object.freeze({
	            Number: "Number",
	            String: "String",
	            Point: "Point",
	            LineString: "LineString",
	            Polygon: "Polygon"
	        }),

	        PartitionKind: Object.freeze({
	            Hash: "Hash"
	        }),

	        ConnectionMode: Object.freeze({
	            Gateway: 0
	        }),

	        QueryCompatibilityMode: Object.freeze({
	            Default: 0,
	            Query: 1,
	            SqlQuery: 2
	        }),

	        /**
	         * Enum for media read mode values.
	         * @readonly
	         * @enum {sting}
	         * @property Buffered Content is buffered at the client and not directly streamed from the content store.
	                              <p>Use Buffered to reduce the time taken to read and write media files.</p>
	         * @property Streamed Content is directly streamed from the content store without any buffering at the client.
	                              <p>Use Streamed to reduce the client memory overhead of reading and writing media files. </p>
	         */
	        MediaReadMode: Object.freeze({
	            Buffered: "Buffered",
	            Streamed: "Streamed"
	        }),

	        /**
	         * Enum for permission mode values.
	         * @readonly
	         * @enum {string}
	         * @property None Permission not valid.
	         * @property Read Permission applicable for read operations only.
	         * @property All Permission applicable for all operations.
	         */
	        PermissionMode: Object.freeze({
	            None: "none",
	            Read: "read",
	            All: "all"
	        }),

	        /**
	         * Enum for trigger type values.
	         * Specifies the type of the trigger.
	         * @readonly
	         * @enum {string}
	         * @property Pre  Trigger should be executed before the associated operation(s).
	         * @property Post Trigger should be executed after the associated operation(s).
	         */
	        TriggerType: Object.freeze({
	            Pre: "pre",
	            Post: "post"
	        }),

	        /**
	         * Enum for trigger operation values.
	         * specifies the operations on which a trigger should be executed.
	         * @readonly
	         * @enum {string}
	         * @property All All operations.
	         * @property Create Create operations only.
	         * @property Update Update operations only.
	         * @property Delete Delete operations only.
	         * @property Replace Replace operations only.
	         */
	        TriggerOperation: Object.freeze({
	            All: "all",
	            Create: "create",
	            Update: "update",
	            Delete: "delete",
	            Replace: "replace"
	        }),

	        /**
	         * Enum for udf type values.
	         * Specifies the types of user defined functions.
	         * @readonly
	         * @enum {string}
	         * @property Javascript Javascript type.
	         */
	        UserDefinedFunctionType: Object.freeze({
	            Javascript: "Javascript"
	        }),

	        /**
	         * @global
	         * Represents the Connection policy associated with a DocumentClient in the Azure Cosmos DB database service.
	         * @property {string} MediaReadMode                - Attachment content (aka media) download mode. Should be one of the values of {@link MediaReadMode}
	         * @property {number} MediaRequestTimeout          - Time to wait for response from network peer for attachment content (aka media) operations. Represented in milliseconds.
	         * @property {number} RequestTimeout               - Request timeout (time to wait for response from network peer). Represented in milliseconds.
	         * @property {bool} EnableEndpointDiscovery        - Flag to enable/disable automatic redirecting of requests based on read/write operations.
	         * @property {Array} PreferredLocations            - List of azure regions to be used as preferred locations for read requests.
	         * @property {RetryOptions} RetryOptions           - RetryOptions instance which defines several configurable properties used during retry.
	         * @property {bool} DisableSSLVerification         - Flag to disable SSL verification for the requests. SSL verification is enabled by default. Don't set this when targeting production endpoints.
	         *                                                   This is intended to be used only when targeting emulator endpoint to avoid failing your requests with SSL related error.
	         * @property {string} ProxyUrl                     - Http/Https proxy url
	        */
	        ConnectionPolicy: Base.defineClass(function() {
	            Object.defineProperty(this, "_defaultRequestTimeout", {
	                value: 60000,
	                writable: true,
	                configurable: true,
	                enumerable: false // this is the default value, so it could be excluded during JSON.stringify
	            });

	            // defaultMediaRequestTimeout is based upon the blob client timeout and the retry policy.
	            Object.defineProperty(this, "_defaultMediaRequestTimeout", {
	                value: 300000,
	                writable: true,
	                configurable: true,
	                enumerable: false // this is the default value, so it could be excluded during JSON.stringify
	            });

	            this.ConnectionMode = AzureDocuments.ConnectionMode.Gateway;
	            this.MediaReadMode = AzureDocuments.MediaReadMode.Buffered;
	            this.MediaRequestTimeout = this._defaultMediaRequestTimeout;
	            this.RequestTimeout = this._defaultRequestTimeout;
	            this.EnableEndpointDiscovery = true;
	            this.PreferredLocations = [];
	            this.RetryOptions = new RetryOptions();
	            this.DisableSSLVerification = false;
	            this.ProxyUrl = "";
	        })
	    }
	);
	//SCRIPT END

	if (true) {
	    module.exports = AzureDocuments;
	}

/***/ }),
/* 105 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4);

	//SCRIPT START
	    /**
	    * Represents the Retry policy assocated with throttled requests in the Azure Cosmos DB database service.
	    * @property {int} [MaxRetryAttemptCount]               - Max number of retries to be performed for a request. Default value 9.
	    * @property {int} [FixedRetryIntervalInMilliseconds]   - Fixed retry interval in milliseconds to wait between each retry ignoring the retryAfter returned as part of the response.
	    * @property {int} [MaxWaitTimeInSeconds]               - Max wait time in seconds to wait for a request while the retries are happening. Default value 30 seconds.
	    */
	    var RetryOptions = Base.defineClass(
	        function RetryOptions(maxRetryAttemptCount, fixedRetryIntervalInMilliseconds, maxWaitTimeInSeconds) {
	            this._maxRetryAttemptCount = maxRetryAttemptCount || 9;
	            this._fixedRetryIntervalInMilliseconds = fixedRetryIntervalInMilliseconds;
	            this._maxWaitTimeInSeconds = maxWaitTimeInSeconds || 30;

	            Object.defineProperty(this, "MaxRetryAttemptCount", {
	                    get: function () {
	                        return this._maxRetryAttemptCount;
	                    },
	                    enumerable: true
	            });

	            Object.defineProperty(this, "FixedRetryIntervalInMilliseconds", {
	                get: function () {
	                    return this._fixedRetryIntervalInMilliseconds;
	                },
	                enumerable: true
	            });

	            Object.defineProperty(this, "MaxWaitTimeInSeconds", {
	                get: function () {
	                    return this._maxWaitTimeInSeconds;
	                },
	                enumerable: true
	            });
	        })
	//SCRIPT END

	if (true) {
	    module.exports = RetryOptions;
	}

/***/ }),
/* 106 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(setImmediate) {/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4),
	    Constants = __webpack_require__(69),
	    ProxyQueryExecutionContext = __webpack_require__(107);

	//SCRIPT START
	var QueryIterator = Base.defineClass(
	    /**
	    * Represents a QueryIterator Object, an implmenetation of feed or query response that enables traversal and iterating over the response
	    * in the Azure Cosmos DB database service.
	    * @class QueryIterator
	    * @param {object} documentclient                - The documentclient object.
	    * @param {SqlQuerySpec | string} query          - A SQL query.
	    * @param {FeedOptions} options                  - Represents the feed options.
	    * @param {callback | callback[]} fetchFunctions - A function to retrieve each page of data. An array of functions may be used to query more than one partition.
	    * @param {string} [resourceLink]                - An optional parameter that represents the resourceLink (will be used in orderby/top/parallel query)
	    */
	    function (documentclient, query, options, fetchFunctions, resourceLink) {

	        this.documentclient = documentclient;
	        this.query = query;
	        this.fetchFunctions = fetchFunctions;
	        this.options = options;
	        this.resourceLink = resourceLink;
	        this.queryExecutionContext = this._createQueryExecutionContext();
	    },
	    {
	        /**
	         * Execute a provided function once per feed element.
	         * @memberof QueryIterator
	         * @instance
	         * @param {callback} callback - Function to execute for each element. the function takes two parameters error, element.
	         * Note: the last element the callback will be called on will be undefined.
	         * If the callback explicitly returned false, the loop gets stopped.
	         */
	        forEach: function(callback) {
	            this.reset();
	            this._forEachImplementation(callback);
	        },

	        /**
	        * Execute a provided function on the next element in the QueryIterator.
	        * @memberof QueryIterator
	        * @instance
	        * @param {callback} callback - Function to execute for each element. the function takes two parameters error, element.
	        */
	        nextItem: function (callback) {
	            this.queryExecutionContext.nextItem(callback);
	        },

	        /**
	         * Retrieve the current element on the QueryIterator.
	         * @memberof QueryIterator
	         * @instance
	         * @param {callback} callback - Function to execute for the current element. the function takes two parameters error, element.
	         */
	        current: function(callback) {
	            this.queryExecutionContext.current(callback);
	        },

	        /**
	         * @deprecated Instead check if callback(undefined, undefined) is invoked by nextItem(callback) or current(callback)
	         *
	         * Determine if there are still remaining resources to processs based on the value of the continuation token or the elements remaining on the current batch in the QueryIterator.
	         * @memberof QueryIterator
	         * @instance
	         * @returns {Boolean} true if there is other elements to process in the QueryIterator.
	         */
	        hasMoreResults: function () {
	            return this.queryExecutionContext.hasMoreResults();
	        },

	        /**
	         * Retrieve all the elements of the feed and pass them as an array to a function
	         * @memberof QueryIterator
	         * @instance
	         * @param {callback} callback - Function execute on the feed response, takes two parameters error, resourcesList
	         */
	        toArray: function (callback) {
	            this.reset();
	            this.toArrayTempResources = [];
	            this._toArrayImplementation(callback);
	        },

	        /**
	         * Retrieve the next batch of the feed and pass them as an array to a function
	         * @memberof QueryIterator
	         * @instance
	         * @param {callback} callback - Function execute on the feed response, takes two parameters error, resourcesList
	         */
	        executeNext: function(callback) {
	            this.queryExecutionContext.fetchMore(function(err, resources, responseHeaders) {
	                if (err) {
	                    return callback(err, undefined, responseHeaders);
	                }

	                callback(undefined, resources, responseHeaders);
	            });
	        },

	        /**
	         * Reset the QueryIterator to the beginning and clear all the resources inside it
	         * @memberof QueryIterator
	         * @instance
	         */
	        reset: function() {
	            this.queryExecutionContext = this._createQueryExecutionContext();
	        },

	        /** @ignore */
	        _toArrayImplementation: function(callback) {
	            var that = this;

	            this.queryExecutionContext.nextItem(function (err, resource, headers) {

	                if (err) {
	                    return callback(err, undefined, headers);
	                }
	                // concatinate the results and fetch more
	                that.toArrayLastResHeaders = headers;

	                if (resource === undefined) {

	                    // no more results
	                    return callback(undefined, that.toArrayTempResources, that.toArrayLastResHeaders);
	                }

	                that.toArrayTempResources.push(resource);

	                setImmediate(function () {
	                    that._toArrayImplementation(callback);
	                });
	            });
	        },

	        /** @ignore */
	        _forEachImplementation: function (callback) {
	            var that = this;
	            this.queryExecutionContext.nextItem(function (err, resource, headers) {
	                if (err) {
	                    return callback(err, undefined, headers);
	                }

	                if (resource === undefined) {
	                    // no more results. This is last iteration
	                    return callback(undefined, undefined, headers);
	                }

	                if (callback(undefined, resource, headers) === false) {
	                    // callback instructed to stop further iteration
	                    return;
	                }

	                // recursively call itself to iterate to the remaining elements
	                setImmediate(function () {
	                    that._forEachImplementation(callback);
	                });
	            });
	        },

	        /** @ignore */
	        _createQueryExecutionContext: function () {
	            return new ProxyQueryExecutionContext(this.documentclient, this.query, this.options, this.fetchFunctions, this.resourceLink);
	        }
	    }
	);
	//SCRIPT END

	if (true) {
	    module.exports = QueryIterator;
	}
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(49).setImmediate))

/***/ }),
/* 107 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , DefaultQueryExecutionContext = __webpack_require__(108)
	    , PipelinedQueryExecutionContext = __webpack_require__(109)
	    , StatusCodes = __webpack_require__(114).StatusCodes
	    , SubStatusCodes = __webpack_require__(114).SubStatusCodes
	    , assert = __webpack_require__(103)

	//SCRIPT START
	var ProxyQueryExecutionContext = Base.defineClass(
	    /**
	     * Represents a ProxyQueryExecutionContext Object. If the query is a partitioned query which can be parallelized it switches the execution context.
	     * @constructor ProxyQueryExecutionContext
	     * @param {object} documentclient                - The documentclient object.
	     * @param {SqlQuerySpec | string} query          - A SQL query.
	     * @param {FeedOptions} options                  - Represents the feed options.
	     * @param {callback | callback[]} fetchFunctions - A function to retrieve each page of data. An array of functions may be used to query more than one partition.
	     * @param {string} [resourceLink]                - collectionLink for parallelized query execution.
	     * @ignore
	    */
	    function (documentclient, query, options, fetchFunctions, resourceLink) {
	        this.documentclient = documentclient;
	        this.query = query;
	        this.fetchFunctions = fetchFunctions;
	        // clone options
	        this.options = JSON.parse(JSON.stringify(options || {}));
	        this.resourceLink = resourceLink;
	        this.queryExecutionContext = new DefaultQueryExecutionContext(this.documentclient, this.query, this.options, this.fetchFunctions);
	    },
	    {
	        /**
	         * Execute a provided function on the next element in the ProxyQueryExecutionContext.
	         * @memberof ProxyQueryExecutionContext
	         * @instance
	         * @param {callback} callback - Function to execute for each element. the function takes two parameters error, element.
	         */
	        nextItem: function (callback) {
	            var that = this;
	            this.queryExecutionContext.nextItem(function (err, resources, headers) {
	                if (err) {
	                    if (that._hasPartitionedExecutionInfo(err)) {
	                        // if that's a partitioned execution info switches the execution context
	                        var partitionedExecutionInfo = that._getParitionedExecutionInfo(err);
	                        that.queryExecutionContext = that._createPipelinedExecutionContext(partitionedExecutionInfo);
	                        return that.nextItem(callback);
	                    } else {
	                        return callback(err, undefined, headers);
	                    }
	                } else {
	                    callback(undefined, resources, headers);
	                }
	            });
	        },

	        _createPipelinedExecutionContext: function (partitionedExecutionInfo) {
	            assert.notStrictEqual(this.resourceLink, undefined, "for top/orderby resourceLink is required.");
	            assert.ok(!Array.isArray(this.resourceLink) || this.resourceLink.length === 1,
	                "for top/orderby exactly one collectionLink is required");

	            var collectionLink = undefined;
	            if (Array.isArray(this.resourceLink)) {
	                collectionLink = this.resourceLink[0];
	            } else {
	                collectionLink = this.resourceLink;
	            }

	            return new PipelinedQueryExecutionContext(
	                this.documentclient,
	                collectionLink,
	                this.query,
	                this.options,
	                partitionedExecutionInfo);
	        },

	        /**
	         * Retrieve the current element on the ProxyQueryExecutionContext.
	         * @memberof ProxyQueryExecutionContext
	         * @instance
	         * @param {callback} callback - Function to execute for the current element. the function takes two parameters error, element.
	         */
	        current: function (callback) {
	            var that = this;
	            this.queryExecutionContext.current(function (err, resources, headers) {
	                if (err) {
	                    if (that._hasPartitionedExecutionInfo(err)) {
	                        // if that's a partitioned execution info switches the execution context
	                        var partitionedExecutionInfo = that._getParitionedExecutionInfo(err);
	                        that.queryExecutionContext = that._createPipelinedExecutionContext(partitionedExecutionInfo);
	                        return that.current(callback);
	                    } else {
	                        return callback(err, undefined, headers);
	                    }
	                } else {
	                    callback(undefined, resources, headers);
	                }
	            });
	        },

	        /**
	         * Determine if there are still remaining resources to process.
	         * @memberof ProxyQueryExecutionContext
	         * @instance
	         * @returns {Boolean} true if there is other elements to process in the ProxyQueryExecutionContext.
	         */
	        hasMoreResults: function () {
	            return this.queryExecutionContext.hasMoreResults();
	        },

	        fetchMore: function (callback) {
	            var that = this;

	            this.queryExecutionContext.fetchMore(function (err, resources, headers) {
	                if (err) {
	                    if (that._hasPartitionedExecutionInfo(err)) {
	                        // if that's a partitioned execution info switches the execution context
	                        var partitionedExecutionInfo = that._getParitionedExecutionInfo(err);
	                        that.queryExecutionContext = that._createPipelinedExecutionContext(partitionedExecutionInfo);
	                        return that.queryExecutionContext.fetchMore(callback);
	                    } else {
	                        return callback(err, undefined, headers);
	                    }
	                } else {
	                    callback(undefined, resources, headers);
	                }
	            });
	        },

	        _hasPartitionedExecutionInfo: function (error) {
	            return (error.code === StatusCodes.BadRequest) && ('substatus' in error) && (error['substatus'] === SubStatusCodes.CrossPartitionQueryNotServable);
	        },

	        _getParitionedExecutionInfo: function (error) {

	            return JSON.parse(JSON.parse(error.body).additionalErrorInfo);
	        },
	    }
	);
	//SCRIPT END

	if (true) {
	    module.exports = ProxyQueryExecutionContext;
	}

/***/ }),
/* 108 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	  , Constants = __webpack_require__(69);

	//SCRIPT START
	var DefaultQueryExecutionContext = Base.defineClass(
	    /**
	     * Provides the basic Query Execution Context. This wraps the internal logic query execution using provided fetch functions
	     * @constructor DefaultQueryExecutionContext
	     * @param {DocumentClient} documentclient        - The service endpoint to use to create the client.
	     * @param {SqlQuerySpec | string} query          - A SQL query.
	     * @param {FeedOptions} [options]                - Represents the feed options.
	     * @param {callback | callback[]} fetchFunctions - A function to retrieve each page of data. An array of functions may be used to query more than one partition.
	     * @ignore
	     */
	    function(documentclient, query, options, fetchFunctions){
	        this.documentclient = documentclient;
	        this.query = query;
	        this.resources = [];
	        this.currentIndex = 0;
	        this.currentPartitionIndex = 0;
	        this.fetchFunctions = (Array.isArray(fetchFunctions)) ? fetchFunctions : [fetchFunctions];
	        this.options = options || {};
	        this.continuation = this.options.continuation || null;
	        this.state = DefaultQueryExecutionContext.STATES.start;
	    },
	    {
	        /**
	         * Execute a provided callback on the next element in the execution context.
	         * @memberof DefaultQueryExecutionContext
	         * @instance
	         * @param {callback} callback - Function to execute for each element. the function takes two parameters error, element.
	         */
	        nextItem: function (callback) {
	            var that = this;
	            this.current(function (err, resources, headers) {
	                ++that.currentIndex;
	                callback(err, resources, headers);
	            });
	        },

	        /**
	         * Retrieve the current element on the execution context.
	         * @memberof DefaultQueryExecutionContext
	         * @instance
	         * @param {callback} callback - Function to execute for the current element. the function takes two parameters error, element.
	         */
	        current: function(callback) {
	            var that = this;
	            if (this.currentIndex < this.resources.length) {
	                return callback(undefined, this.resources[this.currentIndex], undefined);
	            }

	            if (this._canFetchMore()) {
	                this.fetchMore(function (err, resources, headers) {
	                    if (err) {
	                        return callback(err, undefined, headers);
	                    }
	                    
	                    that.resources = resources;
	                    if (that.resources.length === 0) {
	                        if (!that.continuation && that.currentPartitionIndex >= that.fetchFunctions.length) {
	                            that.state = DefaultQueryExecutionContext.STATES.ended;
	                            callback(undefined, undefined, headers);
	                        } else {
	                            that.current(callback);
	                        }
	                        return undefined;
	                    }
	                    callback(undefined, that.resources[that.currentIndex], headers);
	                });
	            } else {
	                this.state = DefaultQueryExecutionContext.STATES.ended;
	                callback(undefined, undefined, undefined);
	            }
	        },

	        /**
	         * Determine if there are still remaining resources to processs based on the value of the continuation token or the elements remaining on the current batch in the execution context.
	         * @memberof DefaultQueryExecutionContext
	         * @instance
	         * @returns {Boolean} true if there is other elements to process in the DefaultQueryExecutionContext.
	         */
	        hasMoreResults: function () {
	            return this.state === DefaultQueryExecutionContext.STATES.start || this.continuation !== undefined || this.currentIndex < this.resources.length || this.currentPartitionIndex < this.fetchFunctions.length;
	        },

	        /**
	         * Fetches the next batch of the feed and pass them as an array to a callback
	         * @memberof DefaultQueryExecutionContext
	         * @instance
	         * @param {callback} callback - Function execute on the feed response, takes two parameters error, resourcesList
	         */
	        fetchMore: function (callback) {
	            if (this.currentPartitionIndex >= this.fetchFunctions.length) {
	                return callback(undefined, undefined, undefined);
	            }
	            var that = this;
	            // Keep to the original continuation and to restore the value after fetchFunction call
	            var originalContinuation = this.options.continuation;
	            this.options.continuation = this.continuation;

	            // Return undefined if there is no more results
	            if (this.currentPartitionIndex >= that.fetchFunctions.length) {
	                return callback(undefined, undefined, undefined);
	            }

	            var fetchFunction = this.fetchFunctions[this.currentPartitionIndex];
	            fetchFunction(this.options, function(err, resources, responseHeaders){
	                if(err) {
	                    that.state = DefaultQueryExecutionContext.STATES.ended;
	                    return callback(err, undefined, responseHeaders);
	                }

	                that.continuation = responseHeaders[Constants.HttpHeaders.Continuation];
	                if (!that.continuation) {
	                    ++that.currentPartitionIndex;
	                }

	                that.state = DefaultQueryExecutionContext.STATES.inProgress;
	                that.currentIndex = 0;
	                that.options.continuation = originalContinuation;
	                callback(undefined, resources, responseHeaders);
	            });
	        },
	        
	        _canFetchMore: function () {
	            var res = (this.state === DefaultQueryExecutionContext.STATES.start
	                || (this.continuation && this.state === DefaultQueryExecutionContext.STATES.inProgress)
	                || (this.currentPartitionIndex < this.fetchFunctions.length
	                    && this.state === DefaultQueryExecutionContext.STATES.inProgress));
	            return res;
	        }
	    }, {

	        STATES:  Object.freeze({ start: "start", inProgress: "inProgress", ended: "ended" })
	    }
	);

	//SCRIPT END

	if (true) {
	    module.exports = DefaultQueryExecutionContext;
	}


/***/ }),
/* 109 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , endpointComponent = __webpack_require__(110)
	    , assert = __webpack_require__(103)
	    , PartitionedQueryExecutionContextInfoParser = __webpack_require__(116)
	    , HeaderUtils = __webpack_require__(113);

	var ParallelQueryExecutionContext = __webpack_require__(117)
	    , OrderByQueryExecutionContext = __webpack_require__(125);

	var AggregateEndpointComponent = endpointComponent.AggregateEndpointComponent
	    , OrderByEndpointComponent = endpointComponent.OrderByEndpointComponent
	    , TopEndpointComponent = endpointComponent.TopEndpointComponent;


	//SCRIPT START
	var PipelinedQueryExecutionContext = Base.defineClass(
	    /**
	     * Provides the PipelinedQueryExecutionContext. It piplelines top and orderby execution context if necessary
	     * @constructor PipelinedQueryExecutionContext
	     * @param {object} documentclient                - The documentclient object.
	     * @param {SqlQuerySpec | string} query          - A SQL query.
	     * @param {FeedOptions} options                  - Represents the feed options.
	     * @param {callback | callback[]} fetchFunctions - A function to retrieve each page of data. An array of functions may be used to query more than one partition.
	     * @param {string} [resourceLink]                - collectionLink for parallelized query execution.
	     * @ignore
	     */
	    function (documentclient, collectionLink, query, options, partitionedQueryExecutionInfo) {
	        this.documentclient = documentclient;
	        this.collectionLink = collectionLink;
	        this.query = query;
	        this.options = options;
	        this.partitionedQueryExecutionInfo = partitionedQueryExecutionInfo;
	        this.endpoint = null;
	        this.pageSize = options["maxItemCount"];
	        if (this.pageSize === undefined) {
	            this.pageSize = PipelinedQueryExecutionContext.DEFAULT_PAGE_SIZE;
	        }
	        
	        // Pick between parallel vs order by execution context
	        var sortOrders = PartitionedQueryExecutionContextInfoParser.parseOrderBy(partitionedQueryExecutionInfo);
	        if (Array.isArray(sortOrders) && sortOrders.length > 0) {
	            // Need to wrap orderby execution context in endpoint component, since the data is nested as a "payload" property.
	            this.endpoint = new OrderByEndpointComponent(
	                new OrderByQueryExecutionContext(
	                    this.documentclient,
	                    this.collectionLink,
	                    this.query,
	                    this.options,
	                    this.partitionedQueryExecutionInfo));
	        } else {
	            this.endpoint = new ParallelQueryExecutionContext(
	                this.documentclient,
	                this.collectionLink,
	                this.query,
	                this.options,
	                this.partitionedQueryExecutionInfo);
	        }
	        
	        // If aggregate then add that to the pipeline
	        var aggregates = PartitionedQueryExecutionContextInfoParser.parseAggregates(partitionedQueryExecutionInfo);
	        if (Array.isArray(aggregates) && aggregates.length > 0) {
	            this.endpoint = new AggregateEndpointComponent(this.endpoint, aggregates);
	        }
	        
	        // If top then add that to the pipeline
	        var top = PartitionedQueryExecutionContextInfoParser.parseTop(partitionedQueryExecutionInfo);
	        if (typeof (top) === 'number') {
	            this.endpoint = new TopEndpointComponent(this.endpoint, top);
	        }
	    },
	    {
	        nextItem: function (callback) {
	            return this.endpoint.nextItem(callback);
	        },

	        current: function (callback) {
	            return this.endpoint.current(callback);
	        },

	        hasMoreResults: function (callback) {
	            return this.endpoint.hasMoreResults(callback);
	        },

	        fetchMore: function (callback) {
	            // if the wrapped endpoint has different implementation for fetchMore use that
	            // otherwise use the default implementation
	            if (typeof this.endpoint.fetchMore === 'function') {
	                return this.endpoint.fetchMore(callback);
	            } else {
	                this._fetchBuffer = [];
	                this._fetchMoreRespHeaders = HeaderUtils.getInitialHeader();
	                return this._fetchMoreImplementation(callback);
	            }
	        },

	        _fetchMoreImplementation: function (callback) {
	            var that = this;
	            this.endpoint.nextItem(function (err, item, headers) {
	                HeaderUtils.mergeHeaders(that._fetchMoreRespHeaders, headers);
	                if (err) {
	                    return callback(err, undefined, that._fetchMoreRespHeaders);
	                }

	                if (item === undefined) {
	                    // no more results
	                    if (that._fetchBuffer.length === 0) {
	                        return callback(undefined, undefined, that._fetchMoreRespHeaders);
	                    } else {
	                        // Just give what we have
	                        var temp = that._fetchBuffer;
	                        that._fetchBuffer = [];
	                        return callback(undefined, temp, that._fetchMoreRespHeaders);
	                    }
	                } else {
	                    // append the result
	                    that._fetchBuffer.push(item);
	                    if (that._fetchBuffer.length >= that.pageSize) {
	                        // fetched enough results
	                        var temp = that._fetchBuffer.slice(0, that.pageSize);
	                        that._fetchBuffer = that._fetchBuffer.splice(that.pageSize);
	                        return callback(undefined, temp, that._fetchMoreRespHeaders);
	                    } else {
	                        // recursively fetch more
	                        that._fetchMoreImplementation(callback);
	                    }
	                }
	            });
	        },
	    },
	    {
	        DEFAULT_PAGE_SIZE: 10
	    }
	);
	//SCRIPT END

	if (true) {
	    module.exports = PipelinedQueryExecutionContext;
	}


/***/ }),
/* 110 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , aggregators = __webpack_require__(111);

	var AverageAggregator = aggregators.AverageAggregator
	    , CountAggregator = aggregators.CountAggregator
	    , MaxAggregator = aggregators.MaxAggregator
	    , MinAggregator = aggregators.MinAggregator
	    , SumAggregator = aggregators.SumAggregator;

	//SCRIPT START
	var OrderByEndpointComponent = Base.defineClass(

	    /**
	     * Represents an endpoint in handling an order by query. For each processed orderby result it returns 'payload' item of the result
	     * @constructor OrderByEndpointComponent
	     * @param {object} executionContext              - Underlying Execution Context
	     * @ignore
	     */
	    function (executionContext) {
	        this.executionContext = executionContext;
	    },
	    {
	         /**
	         * Execute a provided function on the next element in the OrderByEndpointComponent.
	         * @memberof OrderByEndpointComponent
	         * @instance
	         * @param {callback} callback - Function to execute for each element. the function takes two parameters error, element.
	         */
	        nextItem: function (callback) {
	            this.executionContext.nextItem(function (err, item, headers) {
	                if (err) {
	                    return callback(err, undefined, headers);
	                }
	                if (item === undefined) {
	                    return callback(undefined, undefined, headers);
	                }
	                callback(undefined, item["payload"], headers);
	            });
	        },

	        /**
	         * Retrieve the current element on the OrderByEndpointComponent.
	         * @memberof OrderByEndpointComponent
	         * @instance
	         * @param {callback} callback - Function to execute for the current element. the function takes two parameters error, element.
	         */
	        current: function(callback) {
	            this.executionContext.current(function (err, item, headers) {
	                if (err) {
	                    return callback(err, undefined, headers);
	                }
	                if (item === undefined) {
	                    return callback(undefined, undefined, headers);
	                }
	                callback(undefined, item["payload"], headers);
	            });
	        },

	        /**
	         * Determine if there are still remaining resources to processs.
	         * @memberof OrderByEndpointComponent
	         * @instance
	         * @returns {Boolean} true if there is other elements to process in the OrderByEndpointComponent.
	         */
	        hasMoreResults: function () {
	            return this.executionContext.hasMoreResults();
	        },
	    }
	);

	var TopEndpointComponent = Base.defineClass(
	    /**
	     * Represents an endpoint in handling top query. It only returns as many results as top arg specified.
	     * @constructor TopEndpointComponent
	     * @param { object } executionContext - Underlying Execution Context
	     * @ignore
	     */
	    function (executionContext, topCount) {
	        this.executionContext = executionContext;
	        this.topCount = topCount;
	    },
	    {

	        /**
	        * Execute a provided function on the next element in the TopEndpointComponent.
	        * @memberof TopEndpointComponent
	        * @instance
	        * @param {callback} callback - Function to execute for each element. the function takes two parameters error, element.
	        */
	        nextItem: function (callback) {
	            if (this.topCount <= 0) {
	                return callback(undefined, undefined, undefined);
	            }
	            this.topCount--;
	            this.executionContext.nextItem(function (err, item, headers) {
	                callback(err, item, headers);
	            });
	        },

	        /**
	         * Retrieve the current element on the TopEndpointComponent.
	         * @memberof TopEndpointComponent
	         * @instance
	         * @param {callback} callback - Function to execute for the current element. the function takes two parameters error, element.
	         */
	        current: function (callback) {
	            if (this.topCount <= 0) {
	                return callback(undefined, undefined);
	            }
	            this.executionContext.current(function (err, item, headers) {
	                return callback(err, item, headers);
	            });
	        },

	        /**
	         * Determine if there are still remaining resources to processs.
	         * @memberof TopEndpointComponent
	         * @instance
	         * @returns {Boolean} true if there is other elements to process in the TopEndpointComponent.
	         */
	        hasMoreResults: function () {
	            return (this.topCount > 0 && this.executionContext.hasMoreResults());
	        },
	    }
	);

	var AggregateEndpointComponent = Base.defineClass(
	    /**
	     * Represents an endpoint in handling aggregate queries.
	     * @constructor AggregateEndpointComponent
	     * @param { object } executionContext - Underlying Execution Context
	     * @ignore
	     */
	    function (executionContext, aggregateOperators) {
	        this.executionContext = executionContext;
	        this.localAggregators = [];
	        var that = this;
	        aggregateOperators.forEach(function (aggregateOperator) {
	            switch (aggregateOperator) {
	                case 'Average':
	                    that.localAggregators.push(new AverageAggregator());
	                    break;
	                case 'Count':
	                    that.localAggregators.push(new CountAggregator());
	                    break;
	                case 'Max':
	                    that.localAggregators.push(new MaxAggregator());
	                    break;
	                case 'Min':
	                    that.localAggregators.push(new MinAggregator());
	                    break;
	                case 'Sum':
	                    that.localAggregators.push(new SumAggregator());
	                    break;
	            }
	        });
	    },
	    {
	        /**
	        * Populate the aggregated values
	        * @ignore 
	        */
	        _getAggregateResult: function (callback) {
	            this.toArrayTempResources = [];
	            this.aggregateValues = [];
	            this.aggregateValuesIndex = -1;
	            var that = this;

	            this._getQueryResults(function (err, resources) {
	                if (err) {
	                    return callback(err, undefined);
	                }

	                resources.forEach(function (resource) {
	                    that.localAggregators.forEach(function (aggregator) {
	                        var itemValue = undefined;
	                        // Get the value of the first property if it exists
	                        if (resource && Object.keys(resource).length > 0) {
	                            var key = Object.keys(resource)[0];
	                            itemValue = resource[key];
	                        }
	                        aggregator.aggregate(itemValue);
	                    });
	                });

	                // Get the aggregated results
	                that.localAggregators.forEach(function (aggregator) {
	                    that.aggregateValues.push(aggregator.getResult());
	                });

	                return callback(undefined, that.aggregateValues);
	            });
	        },

	        /**
	        * Get the results of queries from all partitions
	        * @ignore 
	        */
	        _getQueryResults: function (callback) {
	            var that = this;

	            this.executionContext.nextItem(function (err, item) {
	                if (err) {
	                    return callback(err, undefined);
	                }
	                
	                if (item === undefined) {
	                    // no more results
	                    return callback(undefined, that.toArrayTempResources);
	                }

	                that.toArrayTempResources = that.toArrayTempResources.concat(item);
	                return that._getQueryResults(callback);
	            });

	        },

	        /**
	        * Execute a provided function on the next element in the AggregateEndpointComponent.
	        * @memberof AggregateEndpointComponent
	        * @instance
	        * @param {callback} callback - Function to execute for each element. the function takes two parameters error, element.
	        */
	        nextItem: function (callback) {
	            var that = this;
	            var _nextItem = function (err, resources) {
	                if (err || that.aggregateValues.length <= 0) {
	                    return callback(undefined, undefined);
	                }

	                var resource = that.aggregateValuesIndex < that.aggregateValues.length
	                    ? that.aggregateValues[++that.aggregateValuesIndex]
	                    : undefined;

	                return callback(undefined, resource);
	            };

	            if (that.aggregateValues == undefined) {
	                that._getAggregateResult(function (err, resources) {
	                    return _nextItem(err, resources);
	                });
	            }
	            else {
	                return _nextItem(undefined, that.aggregateValues);
	            }
	        },

	        /**
	         * Retrieve the current element on the AggregateEndpointComponent.
	         * @memberof AggregateEndpointComponent
	         * @instance
	         * @param {callback} callback - Function to execute for the current element. the function takes two parameters error, element.
	         */
	        current: function (callback) {
	            var that = this;
	            if (that.aggregateValues == undefined) {
	                that._getAggregateResult(function (err, resources) {
	                    return callback(undefined, that.aggregateValues[that.aggregateValuesIndex]);
	                });
	            }
	            else {
	                return callback(undefined, that.aggregateValues[that.aggregateValuesIndex]);
	            }
	        },

	        /**
	         * Determine if there are still remaining resources to processs.
	         * @memberof AggregateEndpointComponent
	         * @instance
	         * @returns {Boolean} true if there is other elements to process in the AggregateEndpointComponent.
	         */
	        hasMoreResults: function () {
	            return this.aggregateValues != null && this.aggregateValuesIndex < this.aggregateValues.length - 1;
	        }
	    }
	);
	//SCRIPT END

	if (true) {
	    exports.OrderByEndpointComponent = OrderByEndpointComponent;
	    exports.TopEndpointComponent = TopEndpointComponent;
	    exports.AggregateEndpointComponent = AggregateEndpointComponent;
	}

/***/ }),
/* 111 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , DocumentProducer = __webpack_require__(112)
	    , OrderByDocumentProducerComparator = __webpack_require__(115);

	//SCRIPT START

	var AverageAggregator = Base.defineClass(

	    /**
	     * Represents an aggregator for AVG operator.
	     * @constructor AverageAggregator
	     * @ignore
	     */
	    function () {
	    },
	    {
	        /**
	        * Add the provided item to aggregation result.
	        * @memberof AverageAggregator
	        * @instance
	        * @param other
	        */
	        aggregate: function (other) {
	            if (other == null || other.sum == null) {
	                return;
	            }
	            if (this.sum == null) {
	                this.sum = 0.0;
	                this.count = 0;
	            }
	            this.sum += other.sum;
	            this.count += other.count;
	        },

	        /**
	        * Get the aggregation result.
	        * @memberof AverageAggregator
	        * @instance
	        */
	        getResult: function () {
	            if (this.sum == null || this.count <= 0) {
	                return undefined;
	            }
	            return this.sum / this.count;
	        }

	    }
	);

	var CountAggregator = Base.defineClass(

	    /**
	     * Represents an aggregator for COUNT operator.
	     * @constructor CountAggregator
	     * @ignore
	     */
	    function () {
	        this.value = 0;
	    },
	    {
	        /**
	        * Add the provided item to aggregation result.
	        * @memberof CountAggregator
	        * @instance
	        * @param other
	        */
	        aggregate: function (other) {
	            this.value += other;
	        },

	        /**
	        * Get the aggregation result.
	        * @memberof CountAggregator
	        * @instance
	        */
	        getResult: function () {
	            return this.value;
	        }

	    }
	);

	var MinAggregator = Base.defineClass(

	    /**
	     * Represents an aggregator for MIN operator.
	     * @constructor MinAggregator
	     * @ignore
	     */
	    function () {
	        this.value = undefined;
	        this.comparer = new OrderByDocumentProducerComparator("Ascending");
	    },
	    {
	        /**
	        * Add the provided item to aggregation result.
	        * @memberof MinAggregator
	        * @instance
	        * @param other
	        */
	        aggregate: function (other) {
	            if (this.value == undefined) {
	                this.value = other;
	            }
	            else {
	                var otherType = other == null ? 'NoValue' : typeof (other);
	                if (this.comparer.compareValue(other, otherType, this.value, typeof (this.value)) < 0) {
	                    this.value = other;
	                }
	            }
	        },

	        /**
	        * Get the aggregation result.
	        * @memberof MinAggregator
	        * @instance
	        */
	        getResult: function () {
	            return this.value;
	        }

	    }
	);

	var MaxAggregator = Base.defineClass(

	    /**
	     * Represents an aggregator for MAX operator.
	     * @constructor MaxAggregator
	     * @ignore
	     */
	    function () {
	        this.value = undefined;
	        this.comparer = new OrderByDocumentProducerComparator("Ascending");
	    },
	    {
	        /**
	        * Add the provided item to aggregation result.
	        * @memberof MaxAggregator
	        * @instance
	        * @param other
	        */
	        aggregate: function (other) {
	            if (this.value == undefined) {
	                this.value = other;
	            }
	            else if (this.comparer.compareValue(other, typeof (other), this.value, typeof (this.value)) > 0) {
	                this.value = other;
	            }
	        },

	        /**
	        * Get the aggregation result.
	        * @memberof MaxAggregator
	        * @instance
	        */
	        getResult: function () {
	            return this.value;
	        }

	    }
	);

	var SumAggregator = Base.defineClass(

	    /**
	     * Represents an aggregator for SUM operator.
	     * @constructor SumAggregator
	     * @ignore
	     */
	    function () {
	    },
	    {
	        /**
	        * Add the provided item to aggregation result.
	        * @memberof SumAggregator
	        * @instance
	        * @param other
	        */
	        aggregate: function (other) {
	            if (other == undefined) {
	                return;
	            }
	            if (this.sum == undefined) {
	                this.sum = other;
	            }
	            else {
	                this.sum += other;
	            }
	        },

	        /**
	        * Get the aggregation result.
	        * @memberof SumAggregator
	        * @instance
	        */
	        getResult: function () {
	            return this.sum;
	        }

	    }
	);
	//SCRIPT END

	if (true) {
	    exports.AverageAggregator = AverageAggregator;
	    exports.CountAggregator = CountAggregator;
	    exports.MinAggregator = MinAggregator;
	    exports.MaxAggregator = MaxAggregator;
	    exports.SumAggregator = SumAggregator;
	}

/***/ }),
/* 112 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , DefaultQueryExecutionContext = __webpack_require__(108)
	    , HttpHeaders = __webpack_require__(69).HttpHeaders
	    , HeaderUtils = __webpack_require__(113)
	    , StatusCodes = __webpack_require__(114).StatusCodes
	    , SubStatusCodes = __webpack_require__(114).SubStatusCodes
	    , assert = __webpack_require__(103)

	//SCRIPT START
	var DocumentProducer = Base.defineClass(
	    /**
	     * Provides the Target Partition Range Query Execution Context.
	     * @constructor DocumentProducer
	     * @param {DocumentClient} documentclient        - The service endpoint to use to create the client.
	     * @param {String} collectionLink                - Represents collection link
	     * @param {SqlQuerySpec | string} query          - A SQL query.
	     * @param {object} targetPartitionKeyRange       - Query Target Partition key Range
	     * @ignore
	     */
	    function (documentclient, collectionLink, query, targetPartitionKeyRange, options) {
	        this.documentclient = documentclient;
	        this.collectionLink = collectionLink;
	        this.query = query;
	        this.targetPartitionKeyRange = targetPartitionKeyRange;
	        this.fetchResults = [];
	        
	        this.state = DocumentProducer.STATES.started;
	        this.allFetched = false;
	        this.err = undefined;
	        
	        this.previousContinuationToken = undefined;
	        this.continuationToken = undefined;
	        this._respHeaders = HeaderUtils.getInitialHeader();
	        
	        var isNameBased = Base.isLinkNameBased(collectionLink);
	        var path = this.documentclient.getPathFromLink(collectionLink, "docs", isNameBased);
	        var id = this.documentclient.getIdFromLink(collectionLink, isNameBased);
	        
	        var that = this;
	        var fetchFunction = function (options, callback) {
	            that.documentclient.queryFeed.call(documentclient,
	                documentclient,
	                path,
	                "docs",
	                id,
	                function (result) { return result.Documents; },
	                function (parent, body) { return body; },
	                query,
	                options,
	                callback,
	                that.targetPartitionKeyRange["id"]);
	        };
	        this.internalExecutionContext = new DefaultQueryExecutionContext(documentclient, query, options, fetchFunction);
	        this.state = DocumentProducer.STATES.inProgress;
	    },
	 {
	        /**
	         * Synchronously gives the contiguous buffered results (stops at the first non result) if any
	         * @returns {Object}       - buffered current items if any
	         * @ignore
	         */
	        peekBufferedItems: function () {
	            var bufferedResults = [];
	            for (var i = 0, done = false; i < this.fetchResults.length && !done; i++) {
	                var fetchResult = this.fetchResults[i];
	                switch (fetchResult.fetchResultType) {
	                    case FetchResultType.Done:
	                        done = true;
	                        break;
	                    case FetchResultType.Exception:
	                        done = true;
	                        break;
	                    case FetchResultType.Result:
	                        bufferedResults.push(fetchResult.feedResponse);
	                        break;
	                }
	            }
	            return bufferedResults;
	        },
	        
	        hasMoreResults: function () {
	            return this.internalExecutionContext.hasMoreResults() || this.fetchResults.length != 0;
	        },
	        
	        gotSplit: function () {
	            var fetchResult = this.fetchResults[0];
	            if (fetchResult.fetchResultType == FetchResultType.Exception) {
	                if (this._needPartitionKeyRangeCacheRefresh(fetchResult.error)) {
	                    return true;
	                }
	            }

	            return false;
	        },
	        
	        /**
	         * Synchronously gives the buffered items if any and moves inner indices.
	         * @returns {Object}       - buffered current items if any
	         * @ignore
	         */
	        consumeBufferedItems: function () {
	            var res = this._getBufferedResults();
	            this.fetchResults = [];
	            this._updateStates(undefined, this.continuationToken === null || this.continuationToken === undefined);
	            return res;
	        },
	        
	        _getAndResetActiveResponseHeaders: function () {
	            var ret = this._respHeaders;
	            this._respHeaders = HeaderUtils.getInitialHeader();
	            return ret;
	        },
	        
	        _updateStates: function (err, allFetched) {
	            if (err) {
	                this.state = DocumentProducer.STATES.ended;
	                this.err = err
	                return;
	            }
	            if (allFetched) {
	                this.allFetched = true;   
	            }
	            if (this.allFetched && this.peekBufferedItems().length === 0) {
	                this.state = DocumentProducer.STATES.ended;
	            }
	            if (this.internalExecutionContext.continuation === this.continuationToken) {
	                // nothing changed
	                return;
	            }
	            this.previousContinuationToken = this.continuationToken;
	            this.continuationToken = this.internalExecutionContext.continuation;
	        },
	        
	        _needPartitionKeyRangeCacheRefresh: function (error) {
	            return (error.code === StatusCodes.Gone) && ('substatus' in error) && (error['substatus'] === SubStatusCodes.PartitionKeyRangeGone);
	        },

	        /**
	         * Fetches and bufferes the next page of results and executes the given callback
	         * @memberof DocumentProducer
	         * @instance
	         * @param {callback} callback - Function to execute for next page of result.
	         *                              the function takes three parameters error, resources, headerResponse.
	        */
	        bufferMore: function (callback) {
	            var that = this;
	            if (that.err) {
	                return callback(that.err);
	            }
	            
	            this.internalExecutionContext.fetchMore(function (err, resources, headerResponse) {
	                if (err) {
	                    if (that._needPartitionKeyRangeCacheRefresh(err)) {
	                        // Split just happend
	                        // Buffer the error so the execution context can still get the feedResponses in the itemBuffer
	                        var bufferedError = new FetchResult(undefined, err);
	                        that.fetchResults.push(bufferedError);
	                        // Putting a dummy result so that the rest of code flows
	                        return callback(undefined, [bufferedError], headerResponse);
	                    }
	                    else {
	                        that._updateStates(err, resources === undefined);
	                        return callback(err, undefined, headerResponse);
	                    }
	                }

	                that._updateStates(undefined, resources === undefined);
	                if (resources != undefined) {
	                    // some more results
	                    resources.forEach(function (element) {
	                        that.fetchResults.push(new FetchResult(element, undefined));
	                    });
	                }

	                return callback(undefined, resources, headerResponse);
	            });
	        },
	        
	        /**
	         * Synchronously gives the bufferend current item if any
	         * @returns {Object}       - buffered current item if any
	         * @ignore
	         */
	        getTargetParitionKeyRange: function () {
	            return this.targetPartitionKeyRange;
	        },

	        /**
	        * Execute a provided function on the next element in the DocumentProducer.
	        * @memberof DocumentProducer
	        * @instance
	        * @param {callback} callback - Function to execute for each element. the function takes two parameters error, element.
	        */
	        nextItem: function (callback) {
	            var that = this;
	            if (that.err) {
	                that._updateStates(err, undefined);
	                return callback(that.err);
	            }

	            this.current(function (err, item, headers) {
	                if (err) {
	                    that._updateStates(err, item === undefined);
	                    return callback(err, undefined, headers);
	                }
	                 
	                var fetchResult = that.fetchResults.shift();
	                that._updateStates(undefined, item === undefined);
	                assert.equal(fetchResult.feedResponse, item);
	                switch (fetchResult.fetchResultType) {
	                    case FetchResultType.Done:
	                        return callback(undefined, undefined, headers);
	                    case FetchResultType.Exception:
	                        return callback(fetchResult.error, undefined, headers);
	                    case FetchResultType.Result:
	                        return callback(undefined, fetchResult.feedResponse, headers);
	                }
	            });
	        },
	        
	        /**
	         * Retrieve the current element on the DocumentProducer.
	         * @memberof DocumentProducer
	         * @instance
	         * @param {callback} callback - Function to execute for the current element. the function takes two parameters error, element.
	         */
	        current: function (callback) {
	            // If something is buffered just give that
	            if (this.fetchResults.length > 0) {
	                var fetchResult = this.fetchResults[0];
	                 //Need to unwrap fetch results
	                switch (fetchResult.fetchResultType) {
	                    case FetchResultType.Done:
	                        return callback(undefined, undefined, this._getAndResetActiveResponseHeaders());
	                    case FetchResultType.Exception:
	                        return callback(fetchResult.error, undefined, this._getAndResetActiveResponseHeaders());
	                    case FetchResultType.Result:
	                        return callback(undefined, fetchResult.feedResponse, this._getAndResetActiveResponseHeaders());
	                }
	            }
	            
	            // If there isn't anymore items left to fetch then let the user know.
	            if (this.allFetched) {
	                return callback(undefined, undefined, this._getAndResetActiveResponseHeaders());
	            }
	            
	            // If there are no more bufferd items and there are still items to be fetched then buffer more
	            var that = this;
	            this.bufferMore(function (err, items, headers) {
	                if (err) {
	                    return callback(err, undefined, headers);
	                }
	                
	                if (items === undefined) {
	                    return callback(undefined, undefined, headers);
	                }
	                HeaderUtils.mergeHeaders(that._respHeaders, headers);
	                
	                that.current(callback);
	            });
	        },
	    },

	    {
	        // Static Members
	        STATES: Object.freeze({ started: "started", inProgress: "inProgress", ended: "ended" })
	    }
	);

	var FetchResultType = {
	    "Done": 0,
	    "Exception": 1,
	    "Result": 2
	};

	var FetchResult = Base.defineClass(
	    /**
	     * Wraps fetch results for the document producer.
	     * This allows the document producer to buffer exceptions so that actual results don't get flushed during splits.
	     * @constructor DocumentProducer
	     * @param {object} feedReponse                  - The response the document producer got back on a successful fetch
	     * @param {object} error                        - The exception meant to be buffered on an unsuccessful fetch
	     * @ignore
	     */
	    function (feedResponse, error) {
	        if (feedResponse) {
	            this.feedResponse = feedResponse;
	            this.fetchResultType = FetchResultType.Result;
	        } else {
	            this.error = error;
	            this.fetchResultType = FetchResultType.Exception;
	        }
	    },
	    {
	    },
	    {
	        DoneResult : {
	            fetchResultType: FetchResultType.Done
	        }
	    }
	);
	//SCRIPT END

	if (true) {
	    module.exports = DocumentProducer;
	}

/***/ }),
/* 113 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , Constants = __webpack_require__(69)
	    , assert = __webpack_require__(103)
	    , util = __webpack_require__(17);

	//SCRIPT START
	var HeaderUtils = Base.defineClass(
	    undefined, undefined,
	    {
	        getRequestChargeIfAny: function (headers) {
	            if (typeof (headers) == 'number') {
	                return headers;
	            } else if (typeof (headers) == 'string') {
	                return parseFloat(headers);
	            }

	            if (headers) {
	                var rc = headers[Constants.HttpHeaders.RequestCharge];
	                if (rc) {
	                    return parseFloat(rc);
	                } else {
	                    return 0;
	                }
	            } else {
	                return 0;
	            }
	        },

	        getInitialHeader: function () {
	            var headers = {};
	            headers[Constants.HttpHeaders.RequestCharge] = 0;
	            return headers;
	        },

	        mergeHeaders: function (headers, toBeMergedHeaders) {
	            if (headers[Constants.HttpHeaders.RequestCharge] == undefined) {
	                headers[Constants.HttpHeaders.RequestCharge] = 0;
	            }
	            if (!toBeMergedHeaders) {
	                return;
	            }
	            headers[Constants.HttpHeaders.RequestCharge] += this.getRequestChargeIfAny(toBeMergedHeaders);
	            if (toBeMergedHeaders[Constants.HttpHeaders.IsRUPerMinuteUsed]) {
	                headers[Constants.HttpHeaders.IsRUPerMinuteUsed] = toBeMergedHeaders[Constants.HttpHeaders.IsRUPerMinuteUsed];
	            }
	        }
	    }
	);
	//SCRIPT END

	if (true) {
	    module.exports = HeaderUtils;
	}

/***/ }),
/* 114 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	//SCRIPT START

	var StatusCodes = {
	    // Success
	    "Ok": 200,
	    "Created": 201,
	    "Accepted": 202,
	    "NoContent": 204,
	    "NotModified": 304,

	    // Client error
	    "BadRequest": 400,
	    "Unauthorized": 401,
	    "Forbidden": 403,
	    "NotFound": 404,
	    "MethodNotAllowed": 405,
	    "RequestTimeout": 408,
	    "Conflict": 409,
	    "Gone": 410,
	    "PreconditionFailed": 412,
	    "RequestEntityTooLarge": 413, 
	    "TooManyRequests": 429,
	    "RetryWith": 449,
	        
	    "InternalServerError": 500,
	    "ServiceUnavailable": 503,

	    //Operation pause and cancel. These are FAKE status codes for QOS logging purpose only.
	    "OperationPaused": 1200,
	    "OperationCancelled": 1201
	};

	var SubStatusCodes = {
	    "Unknown": 0,

	    // 400: Bad Request Substatus 
	    "CrossPartitionQueryNotServable": 1004,

	    // 410: StatusCodeType_Gone: substatus 
	    "PartitionKeyRangeGone": 1002,
	}

	//SCRIPT END

	if (true) {
	    module.exports.StatusCodes = StatusCodes;
	    module.exports.SubStatusCodes = SubStatusCodes;
	}


/***/ }),
/* 115 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , DocumentProducer = __webpack_require__(112)
	    , assert = __webpack_require__(103)
	    , util = __webpack_require__(17);

	//SCRIPT START
	var OrderByDocumentProducerComparator = Base.defineClass(
	    function (sortOrder) {
	        this.sortOrder = sortOrder;
	        this.targetPartitionKeyRangeDocProdComparator = function () {
	            return function (docProd1, docProd2) {
	                var a = docProd1.getTargetParitionKeyRange()['minInclusive'];
	                var b = docProd2.getTargetParitionKeyRange()['minInclusive'];
	                return (a == b ? 0 : (a > b ? 1 : -1));
	            };
	        };
	        
	        this._typeOrdComparator = Object.freeze({
	            NoValue: {
	                ord: 0
	            },
	            undefined: {
	                ord: 1
	            },
	            boolean: {
	                ord: 2,
	                compFunc: function (a, b) {
	                    return (a == b ? 0 : (a > b ? 1 : -1));
	                }
	            },
	            number: {
	                ord: 4,
	                compFunc: function (a, b) {
	                    return (a == b ? 0 : (a > b ? 1 : -1));
	                }
	            },
	            string: {
	                ord: 5,
	                compFunc: function (a, b) {
	                    return (a == b ? 0 : (a > b ? 1 : -1));
	                }
	            }
	        });
	    },
	    {
	        compare: function (docProd1, docProd2) {
	            // Need to check for split, since we don't want to dereference "item" of undefined / exception
	            if (docProd1.gotSplit()) {
	                return -1;
	            }
	            if (docProd2.gotSplit()) {
	                return 1;
	            }

	            var orderByItemsRes1 = this.getOrderByItems(docProd1.peekBufferedItems()[0]);
	            var orderByItemsRes2 = this.getOrderByItems(docProd2.peekBufferedItems()[0]);
	            
	            // validate order by items and types
	            // TODO: once V1 order by on different types is fixed this need to change
	            this.validateOrderByItems(orderByItemsRes1, orderByItemsRes2);
	            
	            // no async call in the for loop
	            for (var i = 0; i < orderByItemsRes1.length; i++) {
	                // compares the orderby items one by one
	                var compRes = this.compareOrderByItem(orderByItemsRes1[i], orderByItemsRes2[i]);
	                if (compRes !== 0) {
	                    if (this.sortOrder[i] === 'Ascending') {
	                        return compRes;
	                    } else if (this.sortOrder[i] === 'Descending') {
	                        return -compRes;
	                    }
	                }
	            }
	            
	            return this.targetPartitionKeyRangeDocProdComparator(docProd1, docProd2);
	        },
	        
	        compareValue: function (item1, type1, item2, type2) {
	            var type1Ord = this._typeOrdComparator[type1].ord;
	            var type2Ord = this._typeOrdComparator[type2].ord;
	            var typeCmp = type1Ord - type2Ord;
	            
	            if (typeCmp !== 0) {
	                // if the types are different, use type ordinal
	                return typeCmp;
	            }
	            
	            // both are of the same type 
	            if ((type1Ord === this._typeOrdComparator['undefined'].ord) || (type1Ord === this._typeOrdComparator['NoValue'].ord)) {
	                // if both types are undefined or Null they are equal
	                return 0;
	            }
	            
	            var compFunc = this._typeOrdComparator[type1].compFunc;
	            assert.notEqual(compFunc, undefined, "cannot find the comparison function");
	            // same type and type is defined compare the items
	            return compFunc(item1, item2);
	        },
	        
	        compareOrderByItem: function (orderByItem1, orderByItem2) {
	            var type1 = this.getType(orderByItem1);
	            var type2 = this.getType(orderByItem2);
	            return this.compareValue(orderByItem1['item'], type1, orderByItem2['item'], type2);
	        },
	        
	        validateOrderByItems: function (res1, res2) {
	            this._throwIf(res1.length != res2.length, util.format("Expected %s, but got %s.", type1, type2));
	            this._throwIf(res1.length != this.sortOrder.length, 'orderByItems cannot have a different size than sort orders.');
	            
	            for (var i = 0; i < this.sortOrder.length; i++) {
	                var type1 = this.getType(res1[i]);
	                var type2 = this.getType(res2[i]);
	                this._throwIf(type1 !== type2, util.format("Expected %s, but got %s.", type1, type2));
	            }
	        },
	        
	        getType: function (orderByItem) {
	            if (!'item' in orderByItem) {
	                return 'NoValue';
	            }
	            var type = typeof (orderByItem['item']);
	            this._throwIf(!type in this._typeOrdComparator, util.format("unrecognizable type %s", type));
	            return type;
	        },
	        
	        getOrderByItems: function (res) {
	            return res['orderByItems'];
	        },
	        
	        _throwIf: function (condition, msg) {
	            if (condition) {
	                throw Error(msg);
	            }
	        }
	    }
	);
	//SCRIPT END

	if (true) {
	    module.exports = OrderByDocumentProducerComparator;
	}

/***/ }),
/* 116 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , assert = __webpack_require__(103)
	    , util = __webpack_require__(17);

	//SCRIPT START
	var PartitionedQueryContants = {
	    QueryInfoPath : 'queryInfo',
	    TopPath: ['queryInfo', 'top'],
	    OrderByPath: ['queryInfo', 'orderBy'],
	    AggregatePath: ['queryInfo', 'aggregates'],
	    QueryRangesPath : 'queryRanges',
	    RewrittenQueryPath: ['queryInfo', 'rewrittenQuery']
	};

	var PartitionedQueryExecutionContextInfoParser = Base.defineClass(
	    undefined, undefined,
	    {
	        parseRewrittenQuery: function (partitionedQueryExecutionInfo) {
	            return this._extract(partitionedQueryExecutionInfo, PartitionedQueryContants.RewrittenQueryPath);
	        },
	        parseQueryRanges: function (partitionedQueryExecutionInfo) {
	            return this._extract(partitionedQueryExecutionInfo, PartitionedQueryContants.QueryRangesPath);
	        },
	        parseOrderBy: function (partitionedQueryExecutionInfo) {
	            return this._extract(partitionedQueryExecutionInfo, PartitionedQueryContants.OrderByPath);
	        },
	        parseAggregates: function (partitionedQueryExecutionInfo) {
	            return this._extract(partitionedQueryExecutionInfo, PartitionedQueryContants.AggregatePath);
	        },
	        parseTop: function (partitionedQueryExecutionInfo) {
	            return this._extract(partitionedQueryExecutionInfo, PartitionedQueryContants.TopPath);
	        },
	        _extract: function (partitionedQueryExecutionInfo, path) {
	            var item = partitionedQueryExecutionInfo;
	            if (typeof (path) === 'string') {
	                return item[path];
	            }
	            assert.ok(Array.isArray(path),
	                util.format("%s is expected to be an array", JSON.stringify(path)));
	            for (var index = 0; index < path.length; index++) {
	                item = item[path[index]];
	                if (item === undefined) {
	                    return undefined;
	                }
	            }
	            return item;
	        }
	    }
	);
	//SCRIPT END

	if (true) {
	    module.exports = PartitionedQueryExecutionContextInfoParser;
	}

/***/ }),
/* 117 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , ParallelQueryExecutionContextBase = __webpack_require__(118)
	    , Constants = __webpack_require__(69)
	    , InMemoryCollectionRoutingMap = __webpack_require__(121)
	    , HeaderUtils = __webpack_require__(113)
	    , assert = __webpack_require__(103);

	var _PartitionKeyRange = InMemoryCollectionRoutingMap._PartitionKeyRange;

	//SCRIPT START

	var ParallelQueryExecutionContext = Base.derive(
	    ParallelQueryExecutionContextBase,
	    /**
	     * Provides the ParallelQueryExecutionContext.
	     * This class is capable of handling parallelized queries and dervives from ParallelQueryExecutionContextBase.
	     *
	     * @constructor ParallelQueryExecutionContext
	     * @param {DocumentClient} documentclient        - The service endpoint to use to create the client.
	     * @param {string} collectionLink                - The Collection Link
	     * @param {FeedOptions} [options]                - Represents the feed options.
	     * @param {object} partitionedQueryExecutionInfo - PartitionedQueryExecutionInfo
	     * @ignore
	     */
	    function (documentclient, collectionLink, query, options, partitionedQueryExecutionInfo) {
	        // Calling on base class constructor
	        ParallelQueryExecutionContextBase.call(this, documentclient, collectionLink, query, options, partitionedQueryExecutionInfo);
	    },
	    {
	        // Instance members are inherited
	        
	        // Overriding documentProducerComparator for ParallelQueryExecutionContexts
	        /**
	         * Provides a Comparator for document producers using the min value of the corresponding target partition.
	         * @returns {object}        - Comparator Function
	         * @ignore
	         */
	        documentProducerComparator: function (docProd1, docProd2) {
	            var a = docProd1.getTargetParitionKeyRange()['minInclusive'];
	            var b = docProd2.getTargetParitionKeyRange()['minInclusive'];
	            return (a == b ? 0 : (a > b ? 1 : -1));
	        },

	        _buildContinuationTokenFrom: function (documentProducer) {
	            // given the document producer constructs the continuation token
	            if (documentProducer.allFetched && documentProducer.peekBufferedItems().length == 0) {
	                return undefined;
	            }
	            
	            
	            var min = documentProducer.targetPartitionKeyRange[_PartitionKeyRange.MinInclusive];
	            var max = documentProducer.targetPartitionKeyRange[_PartitionKeyRange.MaxExclusive];
	            var range = {
	                'min': min,
	                'max': max,
	                'id': documentProducer.targetPartitionKeyRange.id
	            };
	            
	            var withNullDefault = function (token) {
	                if (token) {
	                    return token;
	                } else if (token === null || token === undefined) {
	                    return null;
	                }
	            }
	            
	            var documentProducerContinuationToken = undefined;
	            
	            if (documentProducer.peekBufferedItems().length > 0) {
	                // has unused buffered item so use the previous continuation token
	                documentProducerContinuationToken = documentProducer.previousContinuationToken;
	            } else {
	                documentProducerContinuationToken = documentProducer.continuationToken;
	            }
	            
	            return {
	                'token': withNullDefault(documentProducerContinuationToken),
	                'range': range
	            };
	        },
	    },
	    {
	        // Static members are inherited
	    }
	);

	//SCRIPT END

	if (true) {
	    module.exports = ParallelQueryExecutionContext;
	}

/***/ }),
/* 118 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , Constants = __webpack_require__(69)
	    , PriorityQueue = __webpack_require__(119)
	    , SmartRoutingMapProvider = __webpack_require__(120)
	    , InMemoryCollectionRoutingMap = __webpack_require__(121)
	    , DocumentProducer = __webpack_require__(112)
	    , PartitionedQueryExecutionContextInfoParser = __webpack_require__(116)
	    , bs = __webpack_require__(123)
	    , HeaderUtils = __webpack_require__(113)
	    , semaphore = __webpack_require__(72)
	    , StatusCodes = __webpack_require__(114).StatusCodes
	    , SubStatusCodes = __webpack_require__(114).SubStatusCodes
	    , assert = __webpack_require__(103);

	var QueryRange = InMemoryCollectionRoutingMap.QueryRange;
	var _PartitionKeyRange = InMemoryCollectionRoutingMap._PartitionKeyRange;

	//SCRIPT START

	var ParallelQueryExecutionContextBase = Base.defineClass(
	    /**
	     * Provides the ParallelQueryExecutionContextBase.
	     * This is the base class that ParallelQueryExecutionContext and OrderByQueryExecutionContext will derive from.
	     *
	     * When handling a parallelized query, it instantiates one instance of
	     * DocumentProcuder per target partition key range and aggregates the result of each.
	     *
	     * @constructor ParallelQueryExecutionContext
	     * @param {DocumentClient} documentclient        - The service endpoint to use to create the client.
	     * @param {string} collectionLink                - The Collection Link
	     * @param {FeedOptions} [options]                - Represents the feed options.
	     * @param {object} partitionedQueryExecutionInfo - PartitionedQueryExecutionInfo
	     * @ignore
	     */
	    function (documentclient, collectionLink, query, options, partitionedQueryExecutionInfo) {
	        this.documentclient = documentclient;
	        this.collectionLink = collectionLink;
	        this.query = query;
	        this.options = options;
	        this.partitionedQueryExecutionInfo = partitionedQueryExecutionInfo;
	        
	        this.err = undefined;
	        this.state = ParallelQueryExecutionContextBase.STATES.start;
	        this.routingProvider = new SmartRoutingMapProvider(this.documentclient);
	        this.sortOrders = PartitionedQueryExecutionContextInfoParser.parseOrderBy(this.partitionedQueryExecutionInfo);
	        this.state = ParallelQueryExecutionContextBase.STATES.started;
	        
	        if (options === undefined || options["maxItemCount"] === undefined) {
	            this.pageSize = ParallelQueryExecutionContextBase.DEFAULT_PAGE_SIZE;
	            this.options["maxItemCount"] = this.pageSize;
	        } else {
	            this.pageSize = options["maxItemCount"];
	        }
	        
	        this.requestContinuation = options ? options.continuation : null
	        // response headers of undergoing operation
	        this._respHeaders = HeaderUtils.getInitialHeader();
	        var that = this;
	        
	        // Make priority queue for documentProducers
	        // The comparator is supplied by the derived class
	        this.orderByPQ = new PriorityQueue(function (a, b) { return that.documentProducerComparator(b, a); });
	        // Creating the documentProducers
	        this.sem = new semaphore(1);
	        // Creating callback for semaphore
	        var createDocumentProducersAndFillUpPriorityQueueFunc = function () {
	            // ensure the lock is released after finishing up
	            that._onTargetPartitionRanges(function (err, targetPartitionRanges) {
	                if (err) {
	                    that.err = err;
	                    // release the lock
	                    that.sem.leave();
	                    return;
	                }

	                that.waitingForInternalExecutionContexts = targetPartitionRanges.length;
	                // default to 1 if none is provided.
	                var maxDegreeOfParallelism = options.maxDegreeOfParallelism || 1;
	                if (maxDegreeOfParallelism > 0) {
	                    // at most you will need 1 documentProducer for each partition
	                    maxDegreeOfParallelism = Math.min(maxDegreeOfParallelism, targetPartitionRanges.length)
	                } else {
	                    // if user provided a negative number then we automatically pick 1 documentProducer per partition
	                    maxDegreeOfParallelism = targetPartitionRanges.length;
	                }
	                
	                var parallelismSem = semaphore(maxDegreeOfParallelism);
	                var filteredPartitionKeyRanges = [];
	                // The document producers generated from filteredPartitionKeyRanges
	                var targetPartitionQueryExecutionContextList = [];
	                
	                if (that.requestContinuation) {
	                    // Need to create the first documentProducer with the suppliedCompositeContinuationToken
	                    try {
	                        var suppliedCompositeContinuationToken = JSON.parse(that.requestContinuation);
	                        filteredPartitionKeyRanges = that.getPartitionKeyRangesForContinuation(
	                            suppliedCompositeContinuationToken, targetPartitionRanges
	                        );
	                        if (filteredPartitionKeyRanges.length > 0) {
	                            targetPartitionQueryExecutionContextList.push(
	                                that._createTargetPartitionQueryExecutionContext(
	                                    filteredPartitionKeyRanges[0], suppliedCompositeContinuationToken.token
	                                )
	                            );
	                            // Slicing the first element off, since we already made a documentProducer for it
	                            filteredPartitionKeyRanges = filteredPartitionKeyRanges.slice(1);
	                        }
	                    } catch (e) {
	                        that.err = e;
	                        that.sem.leave();
	                    }
	                } else {
	                    filteredPartitionKeyRanges = targetPartitionRanges;
	                }
	                
	                // Create one documentProducer for each partitionTargetRange
	                filteredPartitionKeyRanges.forEach(
	                    function (partitionTargetRange) {
	                        // no async callback
	                        targetPartitionQueryExecutionContextList.push(
	                            that._createTargetPartitionQueryExecutionContext(partitionTargetRange)
	                        );
	                    }
	                );
	                
	                // Fill up our priority queue with documentProducers
	                targetPartitionQueryExecutionContextList.forEach(
	                    function (documentProducer) {
	                        // has async callback
	                        var throttledFunc = function () {
	                            documentProducer.current(function (err, document, headers) {
	                                try {
	                                    that._mergeWithActiveResponseHeaders(headers);
	                                    if (err) {
	                                        that.err = err;
	                                        return;
	                                    }
	                                    
	                                    if (document == undefined) {
	                                        // no results on this one
	                                        return;
	                                    }
	                                    // if there are matching results in the target ex range add it to the priority queue
	                                    try {
	                                        that.orderByPQ.enq(documentProducer);
	                                    } catch (e) {
	                                        that.err = e;
	                                    }
	                                } finally {
	                                    parallelismSem.leave();
	                                    that._decrementInitiationLock();
	                                }
	                            });
	                        }
	                        parallelismSem.take(throttledFunc);
	                    }
	                );
	            });
	        };
	        this.sem.take(createDocumentProducersAndFillUpPriorityQueueFunc);
	    },

	    {
	        getPartitionKeyRangesForContinuation: function (suppliedCompositeContinuationToken, partitionKeyRanges) {
	            
	            var startRange = {};
	            startRange[_PartitionKeyRange.MinInclusive] = suppliedCompositeContinuationToken.range.min;
	            startRange[_PartitionKeyRange.MaxExclusive] = suppliedCompositeContinuationToken.range.max;
	            
	            var vbCompareFunction = function (x, y) {
	                if (x[_PartitionKeyRange.MinInclusive] > y[_PartitionKeyRange.MinInclusive]) return 1;
	                if (x[_PartitionKeyRange.MinInclusive] < y[_PartitionKeyRange.MinInclusive]) return -1;
	                
	                return 0;
	            }
	            
	            var minIndex = bs.le(partitionKeyRanges, startRange, vbCompareFunction);
	            // that's an error
	            
	            if (minIndex > 0) {
	                throw new Error("BadRequestException: InvalidContinuationToken");
	            }
	            
	            // return slice of the partition key ranges
	            return partitionKeyRanges.slice(minIndex, partitionKeyRanges.length - minIndex);
	        },
	        
	        _decrementInitiationLock: function () {
	            // decrements waitingForInternalExecutionContexts
	            // if waitingForInternalExecutionContexts reaches 0 releases the semaphore and changes the state
	            this.waitingForInternalExecutionContexts = this.waitingForInternalExecutionContexts - 1;
	            if (this.waitingForInternalExecutionContexts === 0) {
	                this.sem.leave();
	                if (this.orderByPQ.size() === 0) {
	                    this.state = ParallelQueryExecutionContextBase.STATES.inProgress;
	                }
	            }
	        },
	        
	        _mergeWithActiveResponseHeaders: function (headers) {
	            HeaderUtils.mergeHeaders(this._respHeaders, headers);
	        },
	        
	        _getAndResetActiveResponseHeaders: function () {
	            var ret = this._respHeaders;
	            this._respHeaders = HeaderUtils.getInitialHeader();
	            return ret;
	        },
	        
	        _onTargetPartitionRanges: function (callback) {
	            // invokes the callback when the target partition ranges are ready
	            var parsedRanges = PartitionedQueryExecutionContextInfoParser.parseQueryRanges(this.partitionedQueryExecutionInfo);
	            var queryRanges = parsedRanges.map(function (item) { return QueryRange.parseFromDict(item); });
	            return this.routingProvider.getOverlappingRanges(callback, this.collectionLink, queryRanges);
	        },

	        /**
	        * Gets the replacement ranges for a partitionkeyrange that has been split
	        * @memberof ParallelQueryExecutionContextBase
	        * @instance
	        */
	        _getReplacementPartitionKeyRanges: function (callback, documentProducer) {
	            var routingMapProvider = this.documentclient.partitionKeyDefinitionCache;
	            var partitionKeyRange = documentProducer.targetPartitionKeyRange;
	            // Download the new routing map
	            this.routingProvider = new SmartRoutingMapProvider(this.documentclient);
	            // Get the queryRange that relates to this partitionKeyRange
	            var queryRange = QueryRange.parsePartitionKeyRange(partitionKeyRange);
	            this.routingProvider.getOverlappingRanges(callback, this.collectionLink, [queryRange]);
	        },
	        
	        /**
	        * Removes the current document producer from the priqueue,
	        * replaces that document producer with child document producers,
	        * then reexecutes the originFunction with the corrrected executionContext
	        * @memberof ParallelQueryExecutionContextBase
	        * @instance
	        */
	        _repairExecutionContext: function (originFunction) {
	            // Get the replacement ranges
	            var that = this;
	            // Removing the invalid documentProducer from the orderByPQ
	            var parentDocumentProducer = that.orderByPQ.deq();
	            var afterReplacementRanges = function (err, replacementPartitionKeyRanges) {
	                if (err) {
	                    that.err = err;
	                    return;
	                }
	                var replacementDocumentProducers = [];
	                // Create the replacement documentProducers
	                replacementPartitionKeyRanges.forEach(function (partitionKeyRange) {
	                    // Create replacment document producers with the parent's continuationToken
	                    var replacementDocumentProducer = that._createTargetPartitionQueryExecutionContext(
	                        partitionKeyRange,
	                        parentDocumentProducer.continuationToken);
	                    replacementDocumentProducers.push(replacementDocumentProducer);
	                });
	                // We need to check if the documentProducers even has anything left to fetch from before enqueing them
	                var checkAndEnqueueDocumentProducer = function (documentProducerToCheck, checkNextDocumentProducerCallback) {
	                    documentProducerToCheck.current(function (err, afterItem, headers) {
	                        if (err) {
	                            // Something actually bad happened
	                            that.err = err;
	                            return;
	                        } else if (afterItem === undefined) {
	                            // no more results left in this document producer, so we don't enqueue it
	                        } else {
	                            // Safe to put document producer back in the queue
	                            that.orderByPQ.enq(documentProducerToCheck);
	                        }

	                        checkNextDocumentProducerCallback();
	                    });
	                };
	                var checkAndEnqueueDocumentProducers = function(replacementDocumentProducers) {
	                    if (replacementDocumentProducers.length > 0) {
	                        // We still have a replacementDocumentProducer to check
	                        var replacementDocumentProducer = replacementDocumentProducers.shift();
	                        checkAndEnqueueDocumentProducer(
	                            replacementDocumentProducer,
	                            function() { checkAndEnqueueDocumentProducers(replacementDocumentProducers); }
	                        );
	                    } else {
	                        // reexecutes the originFunction with the corrrected executionContext
	                        return originFunction();
	                    }
	                }
	                // Invoke the recursive function to get the ball rolling
	                checkAndEnqueueDocumentProducers(replacementDocumentProducers);
	            };
	            this._getReplacementPartitionKeyRanges(afterReplacementRanges, parentDocumentProducer);
	        },
	        
	        _needPartitionKeyRangeCacheRefresh: function (error) {
	            return (error.code === StatusCodes.Gone) && ('substatus' in error) && (error['substatus'] === SubStatusCodes.PartitionKeyRangeGone);
	        },

	        /**
	        * Checks to see if the executionContext needs to be repaired.
	        * if so it repairs the execution context and executes the ifCallback,
	        * else it continues with the current execution context and executes the elseCallback
	        * @memberof ParallelQueryExecutionContextBase
	        * @instance
	        */
	        _repairExecutionContextIfNeeded: function (ifCallback, elseCallback) {
	            var that = this;
	            var documentProducer = that.orderByPQ.peek();
	            // Check if split happened
	            documentProducer.current(function (err, element) {
	                if (err) {
	                    if (that._needPartitionKeyRangeCacheRefresh(err)) {
	                        // Split has happened so we need to repair execution context before continueing
	                        return that._repairExecutionContext(ifCallback);
	                    } else {
	                        // Something actually bad happened ...
	                        that.err = err;
	                        return;
	                    }
	                } else {
	                    // Just continue with the original execution context
	                    return elseCallback();
	                }
	            });
	        },

	        /**
	        * Execute a provided function on the next element in the ParallelQueryExecutionContextBase.
	        * @memberof ParallelQueryExecutionContextBase
	        * @instance
	        * @param {callback} callback - Function to execute for each element. the function takes two parameters error, element.
	        */
	        nextItem: function (callback) {
	            if (this.err) {
	                // if there is a prior error return error
	                return callback(this.err, undefined);
	            }

	            var that = this;
	            this.sem.take(function () {
	                // NOTE: lock must be released before invoking quitting
	                if (that.err) {
	                    // release the lock before invoking callback
	                    that.sem.leave();
	                    // if there is a prior error return error
	                    return callback(that.err, undefined, that._getAndResetActiveResponseHeaders());
	                }
	                
	                if (that.orderByPQ.size() === 0) {
	                    // there is no more results
	                    that.state = ParallelQueryExecutionContextBase.STATES.ended;
	                    // release the lock before invoking callback
	                    that.sem.leave();
	                    return callback(undefined, undefined, that._getAndResetActiveResponseHeaders());
	                }
	                
	                var ifCallback = function () {
	                    // Release the semaphore to avoid deadlock
	                    that.sem.leave();
	                    // Reexcute the function
	                    return that.nextItem(callback);
	                };
	                var elseCallback = function () {
	                    try {
	                        var documentProducer = that.orderByPQ.deq();
	                    } catch (e) {
	                        // if comparing elements of the priority queue throws exception
	                        // set that error and return error
	                        that.err = e;
	                        // release the lock before invoking callback
	                        that.sem.leave();
	                        return callback(that.err, undefined, that._getAndResetActiveResponseHeaders());
	                    }

	                    documentProducer.nextItem(function (err, item, headers) {
	                        that._mergeWithActiveResponseHeaders(headers);
	                        if (err) {
	                            // this should never happen
	                            // because the documentProducer already has buffered an item
	                            // assert err === undefined
	                            that.err =
	                            new Error(
	                                util.format(
	                                    "Extracted DocumentProducer from the priority queue fails to get the buffered item. Due to %s",
	                                    JSON.stringify(err)));
	                            // release the lock before invoking callback
	                            that.sem.leave();
	                            return callback(that.err, undefined, that._getAndResetActiveResponseHeaders());
	                        }
	                        
	                        if (item === undefined) {
	                            // this should never happen
	                            // because the documentProducer already has buffered an item
	                            // assert item !== undefined
	                            that.err =
	                            new Error(
	                                util.format(
	                                    "Extracted DocumentProducer from the priority queue doesn't have any buffered item!"));
	                            // release the lock before invoking callback
	                            that.sem.leave();
	                            return callback(that.err, undefined, that._getAndResetActiveResponseHeaders());
	                        }
	                        // we need to put back the document producer to the queue if it has more elements.
	                        // the lock will be released after we know document producer must be put back in the queue or not
	                        documentProducer.current(function (err, afterItem, headers) {
	                            try {
	                                that._mergeWithActiveResponseHeaders(headers);
	                                if (err) {
	                                    if (that._needPartitionKeyRangeCacheRefresh(err)) {
	                                        // We want the document producer enqueued
	                                        // So that later parts of the code can repair the execution context
	                                        that.orderByPQ.enq(documentProducer);
	                                        return;
	                                    } else {
	                                        // Something actually bad happened
	                                        that.err = err;
	                                        return;
	                                    }
	                                } else if (afterItem === undefined) {
	                                    // no more results is left in this document producer
	                                    return;
	                                } else {
	                                    try {
	                                        var headItem = documentProducer.fetchResults[0];
	                                        assert.notStrictEqual(headItem, undefined,
	                                    'Extracted DocumentProducer from PQ is invalid state with no result!');
	                                        that.orderByPQ.enq(documentProducer);
	                                    } catch (e) {
	                                        // if comparing elements in priority queue throws exception
	                                        // set error
	                                        that.err = e;
	                                    }
	                                    return;
	                                }
	                            } finally {
	                                // release the lock before returning
	                                that.sem.leave();
	                            }
	                        });
	                        
	                        // invoke the callback on the item
	                        return callback(undefined, item, that._getAndResetActiveResponseHeaders());
	                    });
	                }
	                that._repairExecutionContextIfNeeded(ifCallback, elseCallback);
	            });
	        },
	        
	        /**
	         * Retrieve the current element on the ParallelQueryExecutionContextBase.
	         * @memberof ParallelQueryExecutionContextBase
	         * @instance
	         * @param {callback} callback - Function to execute for the current element. the function takes two parameters error, element.
	         */
	         current: function (callback) {
	            if (this.err) {
	                return callback(this.err, undefined, that._getAndResetActiveResponseHeaders());
	            }

	            var that = this;
	            this.sem.take(function () {
	                try {
	                    if (that.err) {
	                        return callback(that.err, undefined, that._getAndResetActiveResponseHeaders());
	                    }
	                    
	                    if (that.orderByPQ.size() === 0) {
	                        return callback(undefined, undefined, that._getAndResetActiveResponseHeaders());
	                    }
	                    
	                    var ifCallback = function () {
	                        // Reexcute the function
	                        return that.current(callback);
	                    };

	                    var elseCallback = function () {
	                        var documentProducer = that.orderByPQ.peek();
	                        documentProducer.current(callback);
	                    };

	                    that._repairExecutionContextIfNeeded(ifCallback, elseCallback);
	                } finally {
	                    that.sem.leave();
	                }
	            });
	        },
	        
	        /**
	         * Determine if there are still remaining resources to processs based on the value of the continuation token or the elements remaining on the current batch in the QueryIterator.
	         * @memberof ParallelQueryExecutionContextBase
	         * @instance
	         * @returns {Boolean} true if there is other elements to process in the ParallelQueryExecutionContextBase.
	         */
	        hasMoreResults: function () {
	            return !(this.state === ParallelQueryExecutionContextBase.STATES.ended || this.err !== undefined);
	        },
	        
	        /**
	         * Creates document producers
	         */
	        _createTargetPartitionQueryExecutionContext: function (partitionKeyTargetRange, continuationToken) {
	            // creates target partition range Query Execution Context
	            var rewrittenQuery = PartitionedQueryExecutionContextInfoParser.parseRewrittenQuery(this.partitionedQueryExecutionInfo);
	            var query = this.query;
	            if (typeof (query) === 'string') {
	                query = { 'query': query };
	            }
	            
	            var formatPlaceHolder = "{documentdb-formattableorderbyquery-filter}";
	            if (rewrittenQuery) {
	                query = JSON.parse(JSON.stringify(query));
	                // We hardcode the formattable filter to true for now
	                rewrittenQuery = rewrittenQuery.replace(formatPlaceHolder, "true");
	                query['query'] = rewrittenQuery;
	            }
	            
	            var options = JSON.parse(JSON.stringify(this.options));
	            if (continuationToken) {
	                options.continuation = continuationToken;
	            } else {
	                options.continuation = undefined;
	            }
	            
	            return new DocumentProducer(this.documentclient, this.collectionLink, query, partitionKeyTargetRange, options);
	        },
	    },

	    {
	        STATES: Object.freeze({ started: "started", inProgress: "inProgress", ended: "ended" }),
	        DEFAULT_PAGE_SIZE: 10
	    }
	);

	//SCRIPT END

	if (true) {
	    module.exports = ParallelQueryExecutionContextBase;
	}


/***/ }),
/* 119 */
/***/ (function(module, exports) {

	/**
	 * Expose `PriorityQueue`.
	 */
	module.exports = PriorityQueue;

	/**
	 * Initializes a new empty `PriorityQueue` with the given `comparator(a, b)`
	 * function, uses `.DEFAULT_COMPARATOR()` when no function is provided.
	 *
	 * The comparator function must return a positive number when `a > b`, 0 when
	 * `a == b` and a negative number when `a < b`.
	 *
	 * @param {Function}
	 * @return {PriorityQueue}
	 * @api public
	 */
	function PriorityQueue(comparator) {
	  this._comparator = comparator || PriorityQueue.DEFAULT_COMPARATOR;
	  this._elements = [];
	}

	/**
	 * Compares `a` and `b`, when `a > b` it returns a positive number, when
	 * it returns 0 and when `a < b` it returns a negative number.
	 *
	 * @param {String|Number} a
	 * @param {String|Number} b
	 * @return {Number}
	 * @api public
	 */
	PriorityQueue.DEFAULT_COMPARATOR = function(a, b) {
	  if (typeof a === 'number' && typeof b === 'number') {
	    return a - b;
	  } else {
	    a = a.toString();
	    b = b.toString();

	    if (a == b) return 0;

	    return (a > b) ? 1 : -1;
	  }
	};

	/**
	 * Returns whether the priority queue is empty or not.
	 *
	 * @return {Boolean}
	 * @api public
	 */
	PriorityQueue.prototype.isEmpty = function() {
	  return this.size() === 0;
	};

	/**
	 * Peeks at the top element of the priority queue.
	 *
	 * @return {Object}
	 * @throws {Error} when the queue is empty.
	 * @api public
	 */
	PriorityQueue.prototype.peek = function() {
	  if (this.isEmpty()) throw new Error('PriorityQueue is empty');

	  return this._elements[0];
	};

	/**
	 * Dequeues the top element of the priority queue.
	 *
	 * @return {Object}
	 * @throws {Error} when the queue is empty.
	 * @api public
	 */
	PriorityQueue.prototype.deq = function() {
	  var first = this.peek();
	  var last = this._elements.pop();
	  var size = this.size();

	  if (size === 0) return first;

	  this._elements[0] = last;
	  var current = 0;

	  while (current < size) {
	    var largest = current;
	    var left = (2 * current) + 1;
	    var right = (2 * current) + 2;

	    if (left < size && this._compare(left, largest) >= 0) {
	      largest = left;
	    }

	    if (right < size && this._compare(right, largest) >= 0) {
	      largest = right;
	    }

	    if (largest === current) break;

	    this._swap(largest, current);
	    current = largest;
	  }

	  return first;
	};

	/**
	 * Enqueues the `element` at the priority queue and returns its new size.
	 *
	 * @param {Object} element
	 * @return {Number}
	 * @api public
	 */
	PriorityQueue.prototype.enq = function(element) {
	  var size = this._elements.push(element);
	  var current = size - 1;

	  while (current > 0) {
	    var parent = Math.floor((current - 1) / 2);

	    if (this._compare(current, parent) <= 0) break;

	    this._swap(parent, current);
	    current = parent;
	  }

	  return size;
	};

	/**
	 * Returns the size of the priority queue.
	 *
	 * @return {Number}
	 * @api public
	 */
	PriorityQueue.prototype.size = function() {
	  return this._elements.length;
	};

	/**
	 *  Iterates over queue elements
	 *
	 *  @param {Function} fn
	 */
	PriorityQueue.prototype.forEach = function(fn) {
	  return this._elements.forEach(fn);
	};

	/**
	 * Compares the values at position `a` and `b` in the priority queue using its
	 * comparator function.
	 *
	 * @param {Number} a
	 * @param {Number} b
	 * @return {Number}
	 * @api private
	 */
	PriorityQueue.prototype._compare = function(a, b) {
	  return this._comparator(this._elements[a], this._elements[b]);
	};

	/**
	 * Swaps the values at position `a` and `b` in the priority queue.
	 *
	 * @param {Number} a
	 * @param {Number} b
	 * @api private
	 */
	PriorityQueue.prototype._swap = function(a, b) {
	  var aux = this._elements[a];
	  this._elements[a] = this._elements[b];
	  this._elements[b] = aux;
	};


/***/ }),
/* 120 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , assert = __webpack_require__(103)
	    , InMemoryCollectionRoutingMap = __webpack_require__(121)
	    , PartitionKeyRangeCache = __webpack_require__(124)
	    , util = __webpack_require__(17);

	var CollectionRoutingMapFactory = InMemoryCollectionRoutingMap.CollectionRoutingMapFactory;
	var QueryRange = InMemoryCollectionRoutingMap.QueryRange;
	var _PartitionKeyRange = InMemoryCollectionRoutingMap._PartitionKeyRange;

	//SCRIPT START
	var SmartRoutingMapProvider = Base.defineClass(
	    
	   /**
	   * Represents a SmartRoutingMapProvider Object,  Efficiently uses PartitionKeyRangeCache and minimizes the unnecessary
	   * invocation of PartitionKeyRangeCache.getOverlappingRanges()
	   * @constructor SmartRoutingMapProvider
	   * @param {object} documentclient                - The documentclient object.
	   * @ignore
	   */
	    function (documentclient) {
	        this._partitionKeyRangeCache = new PartitionKeyRangeCache(documentclient);
	    },
	    {
	        _secondRangeIsAfterFirstRange: function (range1, range2) {
	            assert.notEqual(range1.max, undefined, "invalid arg");
	            assert.notEqual(range2.min, undefined, "invalid arg");

	            if (range1.max > range2.min) {
	                // r.min < #previous_r.max
	                return false;
	            } else {
	                if (range1.max === range2.min && range1.isMaxInclusive && range2.isMinInclusive) {
	                    // the inclusive ending endpoint of previous_r is the same as the inclusive beginning endpoint of r
	                    // they share a point
	                    return false;
	                }
	                return true;
	            }
	        },

	        _isSortedAndNonOverlapping: function (ranges) {
	            for (var idx = 1; idx < ranges.length; idx++) {
	                var previousR = ranges[idx - 1];
	                var r = ranges[idx];
	                if (!this._secondRangeIsAfterFirstRange(previousR, r)) {
	                    return false;
	                }
	            }
	            return true;
	        },

	        _stringMax: function (a, b) {
	            return (a >= b ? a : b);
	        },

	        _stringCompare: function(a, b) {
	            return (a == b ? 0 : (a > b ? 1 : -1));
	        },

	        _subtractRange: function (r, partitionKeyRange) {
	            var left = this._stringMax(partitionKeyRange[_PartitionKeyRange.MaxExclusive], r.min);
	            var leftInclusive;
	            if (this._stringCompare(left, r.min) === 0) {
	                leftInclusive = r.isMinInclusive;
	            } else {
	                leftInclusive = false;
	            }
	            return new QueryRange(left, r.max, leftInclusive,
	                r.isMaxInclusive);
	        },

	        /**
	         * Given the sorted ranges and a collection, invokes the callback on the list of overlapping partition key ranges
	         * @param {callback} callback - Function execute on the overlapping partition key ranges result, takes two parameters error, partition key ranges
	         * @param collectionLink
	         * @param sortedRanges
	         * @ignore
	         */
	        getOverlappingRanges: function (callback, collectionLink, sortedRanges) {
	            // validate if the list is non- overlapping and sorted
	            if (!this._isSortedAndNonOverlapping(sortedRanges)) {
	                return callback(new Error("the list of ranges is not a non-overlapping sorted ranges"), undefined);
	            }

	            var partitionKeyRanges = [];

	            if (sortedRanges.length === 0) {
	                return callback(undefined, partitionKeyRanges);
	            }

	            var that = this;
	            this._partitionKeyRangeCache._onCollectionRoutingMap(function (err, collectionRoutingMap) {
	                if (err) {
	                    return callback(err, undefined);
	                }

	                var index = 0;
	                var currentProvidedRange = sortedRanges[index];
	                while (true) {
	                    if (currentProvidedRange.isEmpty()) {
	                        // skip and go to the next item
	                        if (++index >= sortedRanges.length) {
	                            return callback(undefined, partitionKeyRanges);
	                        }
	                        currentProvidedRange = sortedRanges[index];
	                        continue;
	                    }

	                    var queryRange;
	                    if (partitionKeyRanges.length > 0) {
	                        queryRange = that._subtractRange(
	                            currentProvidedRange, partitionKeyRanges[partitionKeyRanges.length - 1]);
	                    } else {
	                        queryRange = currentProvidedRange;
	                    }

	                    var overlappingRanges = collectionRoutingMap.getOverlappingRanges(queryRange);
	                    assert.ok(overlappingRanges.length > 0, util.format("error: returned overlapping ranges for queryRange %s is empty", queryRange));
	                    partitionKeyRanges = partitionKeyRanges.concat(overlappingRanges);

	                    var lastKnownTargetRange = QueryRange.parsePartitionKeyRange(partitionKeyRanges[partitionKeyRanges.length - 1]);
	                    assert.notEqual(lastKnownTargetRange, undefined);
	                    // the overlapping ranges must contain the requested range
	                    assert.ok(that._stringCompare(currentProvidedRange.max, lastKnownTargetRange.max) <= 0,
	                        util.format("error: returned overlapping ranges %s does not contain the requested range %s", overlappingRanges, queryRange));

	                    // the current range is contained in partitionKeyRanges just move forward
	                    if (++index >= sortedRanges.length) {
	                        return callback(undefined, partitionKeyRanges);
	                    }
	                    currentProvidedRange = sortedRanges[index];

	                    while (that._stringCompare(currentProvidedRange.max, lastKnownTargetRange.max) <= 0) {
	                        // the current range is covered too.just move forward
	                        if (++index >= sortedRanges.length) {
	                            return callback(undefined, partitionKeyRanges);
	                        }
	                        currentProvidedRange = sortedRanges[index];
	                    }
	                }
	            }, collectionLink);
	        }
	    }
	);
	//SCRIPT END

	if (true) {
	    module.exports = SmartRoutingMapProvider;
	}

/***/ }),
/* 121 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , _ = __webpack_require__(122)
	    , bs = __webpack_require__(123)
	    , assert = __webpack_require__(103);

	//SCRIPT START
	var _PartitionKeyRange = {
	    //Partition Key Range Constants
	    MinInclusive : "minInclusive",
	    MaxExclusive : "maxExclusive",
	    Id : "id"
	};

	var _QueryRangeConstants = {
	    //Partition Key Range Constants
	    MinInclusive: "minInclusive",
	    MaxExclusive: "maxExclusive",
	    min: "min"
	};

	var _Constants = {
	    MinimumInclusiveEffectivePartitionKey: "",
	    MaximumExclusiveEffectivePartitionKey: "FF",
	};

	var QueryRange = Base.defineClass(
	    /**
	     * Represents a QueryRange. 
	     * @constructor QueryRange
	     * @param {string} rangeMin                - min
	     * @param {string} rangeMin                - max
	     * @param {boolean} isMinInclusive         - isMinInclusive
	     * @param {boolean} isMaxInclusive         - isMaxInclusive
	     * @ignore
	     */
	    function (rangeMin, rangeMax, isMinInclusive, isMaxInclusive) {
	        this.min = rangeMin;
	        this.max = rangeMax;
	        this.isMinInclusive = isMinInclusive;
	        this.isMaxInclusive = isMaxInclusive;
	    }, 
	    {
	        overlaps: function (other) {
	            var range1 = this;
	            var range2 = other;
	            if (range1 === undefined || range2 === undefined) return false;
	            if (range1.isEmpty() || range2.isEmpty()) return false;

	            if (range1.min <= range2.max || range2.min <= range1.max) {
	                if ((range1.min === range2.max && !(range1.isMinInclusive && range2.isMaxInclusive))
	                    || (range2.min === range1.max && !(range2.isMinInclusive && range1.isMaxInclusive))) {
	                    return false;
	                }
	                return true;
	            }
	            return false;
	        },

	        isEmpty: function () {
	            return (!(this.isMinInclusive && this.isMaxInclusive)) && this.min === this.max;
	        }
	    }, 
	    {
	        /**
	         * Parse a QueryRange from a partitionKeyRange
	         * @returns QueryRange
	         * @ignore
	         */
	        parsePartitionKeyRange: function (partitionKeyRange) {
	            return new QueryRange(partitionKeyRange[_PartitionKeyRange.MinInclusive], partitionKeyRange[_PartitionKeyRange.MaxExclusive],
	                true, false);
	        },
	        /**
	         * Parse a QueryRange from a dictionary
	         * @returns QueryRange
	         * @ignore
	         */
	        parseFromDict: function (queryRangeDict) {
	            return new QueryRange(queryRangeDict.min, queryRangeDict.max, queryRangeDict.isMinInclusive, queryRangeDict.isMaxInclusive);
	        }
	    }
	);

	var InMemoryCollectionRoutingMap = Base.defineClass(
	    /**
	     * Represents a InMemoryCollectionRoutingMap Object, Stores partition key ranges in an efficient way with some additional information and provides
	     * convenience methods for working with set of ranges.
	     */
	    function (rangeById, rangeByInfo, orderedPartitionKeyRanges, orderedPartitionInfo, collectionUniqueId) {
	        this._rangeById = rangeById;
	        this._rangeByInfo = rangeByInfo;
	        this._orderedPartitionKeyRanges = orderedPartitionKeyRanges;
	        this._orderedRanges = orderedPartitionKeyRanges.map(
	            function (pkr) {
	                return new QueryRange(
	                    pkr[_PartitionKeyRange.MinInclusive], pkr[_PartitionKeyRange.MaxExclusive], true, false);
	            });
	        this._orderedPartitionInfo = orderedPartitionInfo;
	        this._collectionUniqueId = collectionUniqueId;
	    },
	    {

	        getOrderedParitionKeyRanges: function () {
	            return this._orderedPartitionKeyRanges;
	        },

	        getRangeByEffectivePartitionKey: function (effectivePartitionKeyValue) {

	            if (_Constants.MinimumInclusiveEffectivePartitionKey === effectivePartitionKeyValue) {
	                return this._orderedPartitionKeyRanges[0];
	            }

	            if (_Constants.MaximumExclusiveEffectivePartitionKey === effectivePartitionKeyValue) {
	                return undefined;
	            }

	            var sortedLow = this._orderedRanges.map(
	                function (r) {
	                    return { v: r.min, b: !r.isMinInclusive };
	                });

	            var index = bs.le(sortedLow, { v: effectivePartitionKeyValue, b: true }, this._vbCompareFunction);
	            // that's an error
	            assert.ok(index >= 0, "error in collection routing map, queried partition key is less than the start range.");
	           
	            return this._orderedPartitionKeyRanges[index];
	        },

	        _vbCompareFunction: function (x, y) {
	            if (x.v > y.v) return 1;
	            if (x.v < y.v) return -1;
	            if (x.b > y.b) return 1;
	            if (x.b < y.b) return -1;
	            return 0;
	        },

	        getRangeByPartitionKeyRangeId: function (partitionKeyRangeId) {

	            var t = this._rangeById[partitionKeyRangeId];

	            if (t === undefined) {
	                return undefined;
	            }
	            return t[0];
	        },

	        getOverlappingRanges: function (providedQueryRanges) {

	            if (!_.isArray(providedQueryRanges)) {
	                return this.getOverlappingRanges([providedQueryRanges]);
	            }
	            
	            var minToPartitionRange = {};
	            var sortedLow = this._orderedRanges.map(
	                function (r) {
	                    return { v: r.min, b: !r.isMinInclusive };
	                });
	            var sortedHigh = this._orderedRanges.map(
	                function (r) {
	                    return { v: r.max, b: r.isMaxInclusive };
	                });

	            // this for loop doesn't invoke any async callback
	            for (var i = 0; i < providedQueryRanges.length; i++) {
	                var queryRange = providedQueryRanges[i];
	                if (queryRange.isEmpty()) {
	                    continue;
	                }
	                var minIndex = bs.le(sortedLow, { v: queryRange.min, b: !queryRange.isMinInclusive }, this._vbCompareFunction);
	                assert.ok(minIndex >= 0, "error in collection routing map, queried value is less than the start range.");

	                var maxIndex = bs.ge(sortedHigh, { v: queryRange.max, b: queryRange.isMaxInclusive }, this._vbCompareFunction);
	                assert.ok(maxIndex < sortedHigh.length, "error in collection routing map, queried value is greater than the end range.");

	                // the for loop doesn't invoke any async callback
	                for (var j = minIndex; j < maxIndex + 1; j++) {
	                    if (queryRange.overlaps(this._orderedRanges[j])) {
	                        minToPartitionRange[this._orderedPartitionKeyRanges[j][_PartitionKeyRange.MinInclusive]] = this._orderedPartitionKeyRanges[j];
	                    }
	                } 
	            }

	            var overlappingPartitionKeyRanges = _.values(minToPartitionRange);

	            var getKey = function (r) {
	                return r[_PartitionKeyRange.MinInclusive];
	            };
	            return _.sortBy(overlappingPartitionKeyRanges, getKey);
	        }
	    }
	);

	var CollectionRoutingMapFactory = Base.defineClass(undefined, undefined,
	    {
	        createCompleteRoutingMap: function (partitionKeyRangeInfoTuppleList, collectionUniqueId) {
	            var rangeById = {};
	            var rangeByInfo = {};

	            var sortedRanges = [];

	            // the for loop doesn't invoke any async callback
	            for (var index = 0; index < partitionKeyRangeInfoTuppleList.length; index++) {
	                var r = partitionKeyRangeInfoTuppleList[index];
	                rangeById[r[0][_PartitionKeyRange.Id]] = r;
	                rangeByInfo[r[1]] = r[0];
	                sortedRanges.push(r);
	            }

	            sortedRanges = _.sortBy(sortedRanges,
	                function (r) {
	                    return r[0][_PartitionKeyRange.MinInclusive];
	                });
	            var partitionKeyOrderedRange = sortedRanges.map(function (r) { return r[0]; });
	            var orderedPartitionInfo = sortedRanges.map(function (r) { return r[1]; });

	            if (!this._isCompleteSetOfRange(partitionKeyOrderedRange)) return undefined;
	            return new InMemoryCollectionRoutingMap(rangeById, rangeByInfo, partitionKeyOrderedRange, orderedPartitionInfo, collectionUniqueId);
	        },

	        _isCompleteSetOfRange: function (partitionKeyOrderedRange) {
	            var isComplete = false;
	            if (partitionKeyOrderedRange.length > 0) {
	                var firstRange = partitionKeyOrderedRange[0];
	                var lastRange = partitionKeyOrderedRange[partitionKeyOrderedRange.length - 1];
	                isComplete = (firstRange[_PartitionKeyRange.MinInclusive] === _Constants.MinimumInclusiveEffectivePartitionKey);
	                isComplete &= (lastRange[_PartitionKeyRange.MaxExclusive] === _Constants.MaximumExclusiveEffectivePartitionKey);

	                for (var i = 1; i < partitionKeyOrderedRange.length; i++) {
	                    var previousRange = partitionKeyOrderedRange[i - 1];
	                    var currentRange = partitionKeyOrderedRange[i];
	                    isComplete &= (previousRange[_PartitionKeyRange.MaxExclusive] == currentRange[_PartitionKeyRange.MinInclusive]);

	                    if (!isComplete) {
	                        if (previousRange[_PartitionKeyRange.MaxExclusive] > currentRange[_PartitionKeyRange.MinInclusive] ) {
	                            throw Error("Ranges overlap");
	                        }
	                        break;
	                    }
	                }
	            }
	            return isComplete;
	        }
	    }
	);
	//SCRIPT END

	if (true) {
	    exports.InMemoryCollectionRoutingMap = InMemoryCollectionRoutingMap;
	    exports.CollectionRoutingMapFactory = CollectionRoutingMapFactory;
	    exports.QueryRange = QueryRange;
	    exports._PartitionKeyRange = _PartitionKeyRange;
	}

/***/ }),
/* 122 */
/***/ (function(module, exports, __webpack_require__) {

	var __WEBPACK_AMD_DEFINE_ARRAY__, __WEBPACK_AMD_DEFINE_RESULT__;//     Underscore.js 1.8.3
	//     http://underscorejs.org
	//     (c) 2009-2015 Jeremy Ashkenas, DocumentCloud and Investigative Reporters & Editors
	//     Underscore may be freely distributed under the MIT license.

	(function() {

	  // Baseline setup
	  // --------------

	  // Establish the root object, `window` in the browser, or `exports` on the server.
	  var root = this;

	  // Save the previous value of the `_` variable.
	  var previousUnderscore = root._;

	  // Save bytes in the minified (but not gzipped) version:
	  var ArrayProto = Array.prototype, ObjProto = Object.prototype, FuncProto = Function.prototype;

	  // Create quick reference variables for speed access to core prototypes.
	  var
	    push             = ArrayProto.push,
	    slice            = ArrayProto.slice,
	    toString         = ObjProto.toString,
	    hasOwnProperty   = ObjProto.hasOwnProperty;

	  // All **ECMAScript 5** native function implementations that we hope to use
	  // are declared here.
	  var
	    nativeIsArray      = Array.isArray,
	    nativeKeys         = Object.keys,
	    nativeBind         = FuncProto.bind,
	    nativeCreate       = Object.create;

	  // Naked function reference for surrogate-prototype-swapping.
	  var Ctor = function(){};

	  // Create a safe reference to the Underscore object for use below.
	  var _ = function(obj) {
	    if (obj instanceof _) return obj;
	    if (!(this instanceof _)) return new _(obj);
	    this._wrapped = obj;
	  };

	  // Export the Underscore object for **Node.js**, with
	  // backwards-compatibility for the old `require()` API. If we're in
	  // the browser, add `_` as a global object.
	  if (true) {
	    if (typeof module !== 'undefined' && module.exports) {
	      exports = module.exports = _;
	    }
	    exports._ = _;
	  } else {
	    root._ = _;
	  }

	  // Current version.
	  _.VERSION = '1.8.3';

	  // Internal function that returns an efficient (for current engines) version
	  // of the passed-in callback, to be repeatedly applied in other Underscore
	  // functions.
	  var optimizeCb = function(func, context, argCount) {
	    if (context === void 0) return func;
	    switch (argCount == null ? 3 : argCount) {
	      case 1: return function(value) {
	        return func.call(context, value);
	      };
	      case 2: return function(value, other) {
	        return func.call(context, value, other);
	      };
	      case 3: return function(value, index, collection) {
	        return func.call(context, value, index, collection);
	      };
	      case 4: return function(accumulator, value, index, collection) {
	        return func.call(context, accumulator, value, index, collection);
	      };
	    }
	    return function() {
	      return func.apply(context, arguments);
	    };
	  };

	  // A mostly-internal function to generate callbacks that can be applied
	  // to each element in a collection, returning the desired result — either
	  // identity, an arbitrary callback, a property matcher, or a property accessor.
	  var cb = function(value, context, argCount) {
	    if (value == null) return _.identity;
	    if (_.isFunction(value)) return optimizeCb(value, context, argCount);
	    if (_.isObject(value)) return _.matcher(value);
	    return _.property(value);
	  };
	  _.iteratee = function(value, context) {
	    return cb(value, context, Infinity);
	  };

	  // An internal function for creating assigner functions.
	  var createAssigner = function(keysFunc, undefinedOnly) {
	    return function(obj) {
	      var length = arguments.length;
	      if (length < 2 || obj == null) return obj;
	      for (var index = 1; index < length; index++) {
	        var source = arguments[index],
	            keys = keysFunc(source),
	            l = keys.length;
	        for (var i = 0; i < l; i++) {
	          var key = keys[i];
	          if (!undefinedOnly || obj[key] === void 0) obj[key] = source[key];
	        }
	      }
	      return obj;
	    };
	  };

	  // An internal function for creating a new object that inherits from another.
	  var baseCreate = function(prototype) {
	    if (!_.isObject(prototype)) return {};
	    if (nativeCreate) return nativeCreate(prototype);
	    Ctor.prototype = prototype;
	    var result = new Ctor;
	    Ctor.prototype = null;
	    return result;
	  };

	  var property = function(key) {
	    return function(obj) {
	      return obj == null ? void 0 : obj[key];
	    };
	  };

	  // Helper for collection methods to determine whether a collection
	  // should be iterated as an array or as an object
	  // Related: http://people.mozilla.org/~jorendorff/es6-draft.html#sec-tolength
	  // Avoids a very nasty iOS 8 JIT bug on ARM-64. #2094
	  var MAX_ARRAY_INDEX = Math.pow(2, 53) - 1;
	  var getLength = property('length');
	  var isArrayLike = function(collection) {
	    var length = getLength(collection);
	    return typeof length == 'number' && length >= 0 && length <= MAX_ARRAY_INDEX;
	  };

	  // Collection Functions
	  // --------------------

	  // The cornerstone, an `each` implementation, aka `forEach`.
	  // Handles raw objects in addition to array-likes. Treats all
	  // sparse array-likes as if they were dense.
	  _.each = _.forEach = function(obj, iteratee, context) {
	    iteratee = optimizeCb(iteratee, context);
	    var i, length;
	    if (isArrayLike(obj)) {
	      for (i = 0, length = obj.length; i < length; i++) {
	        iteratee(obj[i], i, obj);
	      }
	    } else {
	      var keys = _.keys(obj);
	      for (i = 0, length = keys.length; i < length; i++) {
	        iteratee(obj[keys[i]], keys[i], obj);
	      }
	    }
	    return obj;
	  };

	  // Return the results of applying the iteratee to each element.
	  _.map = _.collect = function(obj, iteratee, context) {
	    iteratee = cb(iteratee, context);
	    var keys = !isArrayLike(obj) && _.keys(obj),
	        length = (keys || obj).length,
	        results = Array(length);
	    for (var index = 0; index < length; index++) {
	      var currentKey = keys ? keys[index] : index;
	      results[index] = iteratee(obj[currentKey], currentKey, obj);
	    }
	    return results;
	  };

	  // Create a reducing function iterating left or right.
	  function createReduce(dir) {
	    // Optimized iterator function as using arguments.length
	    // in the main function will deoptimize the, see #1991.
	    function iterator(obj, iteratee, memo, keys, index, length) {
	      for (; index >= 0 && index < length; index += dir) {
	        var currentKey = keys ? keys[index] : index;
	        memo = iteratee(memo, obj[currentKey], currentKey, obj);
	      }
	      return memo;
	    }

	    return function(obj, iteratee, memo, context) {
	      iteratee = optimizeCb(iteratee, context, 4);
	      var keys = !isArrayLike(obj) && _.keys(obj),
	          length = (keys || obj).length,
	          index = dir > 0 ? 0 : length - 1;
	      // Determine the initial value if none is provided.
	      if (arguments.length < 3) {
	        memo = obj[keys ? keys[index] : index];
	        index += dir;
	      }
	      return iterator(obj, iteratee, memo, keys, index, length);
	    };
	  }

	  // **Reduce** builds up a single result from a list of values, aka `inject`,
	  // or `foldl`.
	  _.reduce = _.foldl = _.inject = createReduce(1);

	  // The right-associative version of reduce, also known as `foldr`.
	  _.reduceRight = _.foldr = createReduce(-1);

	  // Return the first value which passes a truth test. Aliased as `detect`.
	  _.find = _.detect = function(obj, predicate, context) {
	    var key;
	    if (isArrayLike(obj)) {
	      key = _.findIndex(obj, predicate, context);
	    } else {
	      key = _.findKey(obj, predicate, context);
	    }
	    if (key !== void 0 && key !== -1) return obj[key];
	  };

	  // Return all the elements that pass a truth test.
	  // Aliased as `select`.
	  _.filter = _.select = function(obj, predicate, context) {
	    var results = [];
	    predicate = cb(predicate, context);
	    _.each(obj, function(value, index, list) {
	      if (predicate(value, index, list)) results.push(value);
	    });
	    return results;
	  };

	  // Return all the elements for which a truth test fails.
	  _.reject = function(obj, predicate, context) {
	    return _.filter(obj, _.negate(cb(predicate)), context);
	  };

	  // Determine whether all of the elements match a truth test.
	  // Aliased as `all`.
	  _.every = _.all = function(obj, predicate, context) {
	    predicate = cb(predicate, context);
	    var keys = !isArrayLike(obj) && _.keys(obj),
	        length = (keys || obj).length;
	    for (var index = 0; index < length; index++) {
	      var currentKey = keys ? keys[index] : index;
	      if (!predicate(obj[currentKey], currentKey, obj)) return false;
	    }
	    return true;
	  };

	  // Determine if at least one element in the object matches a truth test.
	  // Aliased as `any`.
	  _.some = _.any = function(obj, predicate, context) {
	    predicate = cb(predicate, context);
	    var keys = !isArrayLike(obj) && _.keys(obj),
	        length = (keys || obj).length;
	    for (var index = 0; index < length; index++) {
	      var currentKey = keys ? keys[index] : index;
	      if (predicate(obj[currentKey], currentKey, obj)) return true;
	    }
	    return false;
	  };

	  // Determine if the array or object contains a given item (using `===`).
	  // Aliased as `includes` and `include`.
	  _.contains = _.includes = _.include = function(obj, item, fromIndex, guard) {
	    if (!isArrayLike(obj)) obj = _.values(obj);
	    if (typeof fromIndex != 'number' || guard) fromIndex = 0;
	    return _.indexOf(obj, item, fromIndex) >= 0;
	  };

	  // Invoke a method (with arguments) on every item in a collection.
	  _.invoke = function(obj, method) {
	    var args = slice.call(arguments, 2);
	    var isFunc = _.isFunction(method);
	    return _.map(obj, function(value) {
	      var func = isFunc ? method : value[method];
	      return func == null ? func : func.apply(value, args);
	    });
	  };

	  // Convenience version of a common use case of `map`: fetching a property.
	  _.pluck = function(obj, key) {
	    return _.map(obj, _.property(key));
	  };

	  // Convenience version of a common use case of `filter`: selecting only objects
	  // containing specific `key:value` pairs.
	  _.where = function(obj, attrs) {
	    return _.filter(obj, _.matcher(attrs));
	  };

	  // Convenience version of a common use case of `find`: getting the first object
	  // containing specific `key:value` pairs.
	  _.findWhere = function(obj, attrs) {
	    return _.find(obj, _.matcher(attrs));
	  };

	  // Return the maximum element (or element-based computation).
	  _.max = function(obj, iteratee, context) {
	    var result = -Infinity, lastComputed = -Infinity,
	        value, computed;
	    if (iteratee == null && obj != null) {
	      obj = isArrayLike(obj) ? obj : _.values(obj);
	      for (var i = 0, length = obj.length; i < length; i++) {
	        value = obj[i];
	        if (value > result) {
	          result = value;
	        }
	      }
	    } else {
	      iteratee = cb(iteratee, context);
	      _.each(obj, function(value, index, list) {
	        computed = iteratee(value, index, list);
	        if (computed > lastComputed || computed === -Infinity && result === -Infinity) {
	          result = value;
	          lastComputed = computed;
	        }
	      });
	    }
	    return result;
	  };

	  // Return the minimum element (or element-based computation).
	  _.min = function(obj, iteratee, context) {
	    var result = Infinity, lastComputed = Infinity,
	        value, computed;
	    if (iteratee == null && obj != null) {
	      obj = isArrayLike(obj) ? obj : _.values(obj);
	      for (var i = 0, length = obj.length; i < length; i++) {
	        value = obj[i];
	        if (value < result) {
	          result = value;
	        }
	      }
	    } else {
	      iteratee = cb(iteratee, context);
	      _.each(obj, function(value, index, list) {
	        computed = iteratee(value, index, list);
	        if (computed < lastComputed || computed === Infinity && result === Infinity) {
	          result = value;
	          lastComputed = computed;
	        }
	      });
	    }
	    return result;
	  };

	  // Shuffle a collection, using the modern version of the
	  // [Fisher-Yates shuffle](http://en.wikipedia.org/wiki/Fisher–Yates_shuffle).
	  _.shuffle = function(obj) {
	    var set = isArrayLike(obj) ? obj : _.values(obj);
	    var length = set.length;
	    var shuffled = Array(length);
	    for (var index = 0, rand; index < length; index++) {
	      rand = _.random(0, index);
	      if (rand !== index) shuffled[index] = shuffled[rand];
	      shuffled[rand] = set[index];
	    }
	    return shuffled;
	  };

	  // Sample **n** random values from a collection.
	  // If **n** is not specified, returns a single random element.
	  // The internal `guard` argument allows it to work with `map`.
	  _.sample = function(obj, n, guard) {
	    if (n == null || guard) {
	      if (!isArrayLike(obj)) obj = _.values(obj);
	      return obj[_.random(obj.length - 1)];
	    }
	    return _.shuffle(obj).slice(0, Math.max(0, n));
	  };

	  // Sort the object's values by a criterion produced by an iteratee.
	  _.sortBy = function(obj, iteratee, context) {
	    iteratee = cb(iteratee, context);
	    return _.pluck(_.map(obj, function(value, index, list) {
	      return {
	        value: value,
	        index: index,
	        criteria: iteratee(value, index, list)
	      };
	    }).sort(function(left, right) {
	      var a = left.criteria;
	      var b = right.criteria;
	      if (a !== b) {
	        if (a > b || a === void 0) return 1;
	        if (a < b || b === void 0) return -1;
	      }
	      return left.index - right.index;
	    }), 'value');
	  };

	  // An internal function used for aggregate "group by" operations.
	  var group = function(behavior) {
	    return function(obj, iteratee, context) {
	      var result = {};
	      iteratee = cb(iteratee, context);
	      _.each(obj, function(value, index) {
	        var key = iteratee(value, index, obj);
	        behavior(result, value, key);
	      });
	      return result;
	    };
	  };

	  // Groups the object's values by a criterion. Pass either a string attribute
	  // to group by, or a function that returns the criterion.
	  _.groupBy = group(function(result, value, key) {
	    if (_.has(result, key)) result[key].push(value); else result[key] = [value];
	  });

	  // Indexes the object's values by a criterion, similar to `groupBy`, but for
	  // when you know that your index values will be unique.
	  _.indexBy = group(function(result, value, key) {
	    result[key] = value;
	  });

	  // Counts instances of an object that group by a certain criterion. Pass
	  // either a string attribute to count by, or a function that returns the
	  // criterion.
	  _.countBy = group(function(result, value, key) {
	    if (_.has(result, key)) result[key]++; else result[key] = 1;
	  });

	  // Safely create a real, live array from anything iterable.
	  _.toArray = function(obj) {
	    if (!obj) return [];
	    if (_.isArray(obj)) return slice.call(obj);
	    if (isArrayLike(obj)) return _.map(obj, _.identity);
	    return _.values(obj);
	  };

	  // Return the number of elements in an object.
	  _.size = function(obj) {
	    if (obj == null) return 0;
	    return isArrayLike(obj) ? obj.length : _.keys(obj).length;
	  };

	  // Split a collection into two arrays: one whose elements all satisfy the given
	  // predicate, and one whose elements all do not satisfy the predicate.
	  _.partition = function(obj, predicate, context) {
	    predicate = cb(predicate, context);
	    var pass = [], fail = [];
	    _.each(obj, function(value, key, obj) {
	      (predicate(value, key, obj) ? pass : fail).push(value);
	    });
	    return [pass, fail];
	  };

	  // Array Functions
	  // ---------------

	  // Get the first element of an array. Passing **n** will return the first N
	  // values in the array. Aliased as `head` and `take`. The **guard** check
	  // allows it to work with `_.map`.
	  _.first = _.head = _.take = function(array, n, guard) {
	    if (array == null) return void 0;
	    if (n == null || guard) return array[0];
	    return _.initial(array, array.length - n);
	  };

	  // Returns everything but the last entry of the array. Especially useful on
	  // the arguments object. Passing **n** will return all the values in
	  // the array, excluding the last N.
	  _.initial = function(array, n, guard) {
	    return slice.call(array, 0, Math.max(0, array.length - (n == null || guard ? 1 : n)));
	  };

	  // Get the last element of an array. Passing **n** will return the last N
	  // values in the array.
	  _.last = function(array, n, guard) {
	    if (array == null) return void 0;
	    if (n == null || guard) return array[array.length - 1];
	    return _.rest(array, Math.max(0, array.length - n));
	  };

	  // Returns everything but the first entry of the array. Aliased as `tail` and `drop`.
	  // Especially useful on the arguments object. Passing an **n** will return
	  // the rest N values in the array.
	  _.rest = _.tail = _.drop = function(array, n, guard) {
	    return slice.call(array, n == null || guard ? 1 : n);
	  };

	  // Trim out all falsy values from an array.
	  _.compact = function(array) {
	    return _.filter(array, _.identity);
	  };

	  // Internal implementation of a recursive `flatten` function.
	  var flatten = function(input, shallow, strict, startIndex) {
	    var output = [], idx = 0;
	    for (var i = startIndex || 0, length = getLength(input); i < length; i++) {
	      var value = input[i];
	      if (isArrayLike(value) && (_.isArray(value) || _.isArguments(value))) {
	        //flatten current level of array or arguments object
	        if (!shallow) value = flatten(value, shallow, strict);
	        var j = 0, len = value.length;
	        output.length += len;
	        while (j < len) {
	          output[idx++] = value[j++];
	        }
	      } else if (!strict) {
	        output[idx++] = value;
	      }
	    }
	    return output;
	  };

	  // Flatten out an array, either recursively (by default), or just one level.
	  _.flatten = function(array, shallow) {
	    return flatten(array, shallow, false);
	  };

	  // Return a version of the array that does not contain the specified value(s).
	  _.without = function(array) {
	    return _.difference(array, slice.call(arguments, 1));
	  };

	  // Produce a duplicate-free version of the array. If the array has already
	  // been sorted, you have the option of using a faster algorithm.
	  // Aliased as `unique`.
	  _.uniq = _.unique = function(array, isSorted, iteratee, context) {
	    if (!_.isBoolean(isSorted)) {
	      context = iteratee;
	      iteratee = isSorted;
	      isSorted = false;
	    }
	    if (iteratee != null) iteratee = cb(iteratee, context);
	    var result = [];
	    var seen = [];
	    for (var i = 0, length = getLength(array); i < length; i++) {
	      var value = array[i],
	          computed = iteratee ? iteratee(value, i, array) : value;
	      if (isSorted) {
	        if (!i || seen !== computed) result.push(value);
	        seen = computed;
	      } else if (iteratee) {
	        if (!_.contains(seen, computed)) {
	          seen.push(computed);
	          result.push(value);
	        }
	      } else if (!_.contains(result, value)) {
	        result.push(value);
	      }
	    }
	    return result;
	  };

	  // Produce an array that contains the union: each distinct element from all of
	  // the passed-in arrays.
	  _.union = function() {
	    return _.uniq(flatten(arguments, true, true));
	  };

	  // Produce an array that contains every item shared between all the
	  // passed-in arrays.
	  _.intersection = function(array) {
	    var result = [];
	    var argsLength = arguments.length;
	    for (var i = 0, length = getLength(array); i < length; i++) {
	      var item = array[i];
	      if (_.contains(result, item)) continue;
	      for (var j = 1; j < argsLength; j++) {
	        if (!_.contains(arguments[j], item)) break;
	      }
	      if (j === argsLength) result.push(item);
	    }
	    return result;
	  };

	  // Take the difference between one array and a number of other arrays.
	  // Only the elements present in just the first array will remain.
	  _.difference = function(array) {
	    var rest = flatten(arguments, true, true, 1);
	    return _.filter(array, function(value){
	      return !_.contains(rest, value);
	    });
	  };

	  // Zip together multiple lists into a single array -- elements that share
	  // an index go together.
	  _.zip = function() {
	    return _.unzip(arguments);
	  };

	  // Complement of _.zip. Unzip accepts an array of arrays and groups
	  // each array's elements on shared indices
	  _.unzip = function(array) {
	    var length = array && _.max(array, getLength).length || 0;
	    var result = Array(length);

	    for (var index = 0; index < length; index++) {
	      result[index] = _.pluck(array, index);
	    }
	    return result;
	  };

	  // Converts lists into objects. Pass either a single array of `[key, value]`
	  // pairs, or two parallel arrays of the same length -- one of keys, and one of
	  // the corresponding values.
	  _.object = function(list, values) {
	    var result = {};
	    for (var i = 0, length = getLength(list); i < length; i++) {
	      if (values) {
	        result[list[i]] = values[i];
	      } else {
	        result[list[i][0]] = list[i][1];
	      }
	    }
	    return result;
	  };

	  // Generator function to create the findIndex and findLastIndex functions
	  function createPredicateIndexFinder(dir) {
	    return function(array, predicate, context) {
	      predicate = cb(predicate, context);
	      var length = getLength(array);
	      var index = dir > 0 ? 0 : length - 1;
	      for (; index >= 0 && index < length; index += dir) {
	        if (predicate(array[index], index, array)) return index;
	      }
	      return -1;
	    };
	  }

	  // Returns the first index on an array-like that passes a predicate test
	  _.findIndex = createPredicateIndexFinder(1);
	  _.findLastIndex = createPredicateIndexFinder(-1);

	  // Use a comparator function to figure out the smallest index at which
	  // an object should be inserted so as to maintain order. Uses binary search.
	  _.sortedIndex = function(array, obj, iteratee, context) {
	    iteratee = cb(iteratee, context, 1);
	    var value = iteratee(obj);
	    var low = 0, high = getLength(array);
	    while (low < high) {
	      var mid = Math.floor((low + high) / 2);
	      if (iteratee(array[mid]) < value) low = mid + 1; else high = mid;
	    }
	    return low;
	  };

	  // Generator function to create the indexOf and lastIndexOf functions
	  function createIndexFinder(dir, predicateFind, sortedIndex) {
	    return function(array, item, idx) {
	      var i = 0, length = getLength(array);
	      if (typeof idx == 'number') {
	        if (dir > 0) {
	            i = idx >= 0 ? idx : Math.max(idx + length, i);
	        } else {
	            length = idx >= 0 ? Math.min(idx + 1, length) : idx + length + 1;
	        }
	      } else if (sortedIndex && idx && length) {
	        idx = sortedIndex(array, item);
	        return array[idx] === item ? idx : -1;
	      }
	      if (item !== item) {
	        idx = predicateFind(slice.call(array, i, length), _.isNaN);
	        return idx >= 0 ? idx + i : -1;
	      }
	      for (idx = dir > 0 ? i : length - 1; idx >= 0 && idx < length; idx += dir) {
	        if (array[idx] === item) return idx;
	      }
	      return -1;
	    };
	  }

	  // Return the position of the first occurrence of an item in an array,
	  // or -1 if the item is not included in the array.
	  // If the array is large and already in sort order, pass `true`
	  // for **isSorted** to use binary search.
	  _.indexOf = createIndexFinder(1, _.findIndex, _.sortedIndex);
	  _.lastIndexOf = createIndexFinder(-1, _.findLastIndex);

	  // Generate an integer Array containing an arithmetic progression. A port of
	  // the native Python `range()` function. See
	  // [the Python documentation](http://docs.python.org/library/functions.html#range).
	  _.range = function(start, stop, step) {
	    if (stop == null) {
	      stop = start || 0;
	      start = 0;
	    }
	    step = step || 1;

	    var length = Math.max(Math.ceil((stop - start) / step), 0);
	    var range = Array(length);

	    for (var idx = 0; idx < length; idx++, start += step) {
	      range[idx] = start;
	    }

	    return range;
	  };

	  // Function (ahem) Functions
	  // ------------------

	  // Determines whether to execute a function as a constructor
	  // or a normal function with the provided arguments
	  var executeBound = function(sourceFunc, boundFunc, context, callingContext, args) {
	    if (!(callingContext instanceof boundFunc)) return sourceFunc.apply(context, args);
	    var self = baseCreate(sourceFunc.prototype);
	    var result = sourceFunc.apply(self, args);
	    if (_.isObject(result)) return result;
	    return self;
	  };

	  // Create a function bound to a given object (assigning `this`, and arguments,
	  // optionally). Delegates to **ECMAScript 5**'s native `Function.bind` if
	  // available.
	  _.bind = function(func, context) {
	    if (nativeBind && func.bind === nativeBind) return nativeBind.apply(func, slice.call(arguments, 1));
	    if (!_.isFunction(func)) throw new TypeError('Bind must be called on a function');
	    var args = slice.call(arguments, 2);
	    var bound = function() {
	      return executeBound(func, bound, context, this, args.concat(slice.call(arguments)));
	    };
	    return bound;
	  };

	  // Partially apply a function by creating a version that has had some of its
	  // arguments pre-filled, without changing its dynamic `this` context. _ acts
	  // as a placeholder, allowing any combination of arguments to be pre-filled.
	  _.partial = function(func) {
	    var boundArgs = slice.call(arguments, 1);
	    var bound = function() {
	      var position = 0, length = boundArgs.length;
	      var args = Array(length);
	      for (var i = 0; i < length; i++) {
	        args[i] = boundArgs[i] === _ ? arguments[position++] : boundArgs[i];
	      }
	      while (position < arguments.length) args.push(arguments[position++]);
	      return executeBound(func, bound, this, this, args);
	    };
	    return bound;
	  };

	  // Bind a number of an object's methods to that object. Remaining arguments
	  // are the method names to be bound. Useful for ensuring that all callbacks
	  // defined on an object belong to it.
	  _.bindAll = function(obj) {
	    var i, length = arguments.length, key;
	    if (length <= 1) throw new Error('bindAll must be passed function names');
	    for (i = 1; i < length; i++) {
	      key = arguments[i];
	      obj[key] = _.bind(obj[key], obj);
	    }
	    return obj;
	  };

	  // Memoize an expensive function by storing its results.
	  _.memoize = function(func, hasher) {
	    var memoize = function(key) {
	      var cache = memoize.cache;
	      var address = '' + (hasher ? hasher.apply(this, arguments) : key);
	      if (!_.has(cache, address)) cache[address] = func.apply(this, arguments);
	      return cache[address];
	    };
	    memoize.cache = {};
	    return memoize;
	  };

	  // Delays a function for the given number of milliseconds, and then calls
	  // it with the arguments supplied.
	  _.delay = function(func, wait) {
	    var args = slice.call(arguments, 2);
	    return setTimeout(function(){
	      return func.apply(null, args);
	    }, wait);
	  };

	  // Defers a function, scheduling it to run after the current call stack has
	  // cleared.
	  _.defer = _.partial(_.delay, _, 1);

	  // Returns a function, that, when invoked, will only be triggered at most once
	  // during a given window of time. Normally, the throttled function will run
	  // as much as it can, without ever going more than once per `wait` duration;
	  // but if you'd like to disable the execution on the leading edge, pass
	  // `{leading: false}`. To disable execution on the trailing edge, ditto.
	  _.throttle = function(func, wait, options) {
	    var context, args, result;
	    var timeout = null;
	    var previous = 0;
	    if (!options) options = {};
	    var later = function() {
	      previous = options.leading === false ? 0 : _.now();
	      timeout = null;
	      result = func.apply(context, args);
	      if (!timeout) context = args = null;
	    };
	    return function() {
	      var now = _.now();
	      if (!previous && options.leading === false) previous = now;
	      var remaining = wait - (now - previous);
	      context = this;
	      args = arguments;
	      if (remaining <= 0 || remaining > wait) {
	        if (timeout) {
	          clearTimeout(timeout);
	          timeout = null;
	        }
	        previous = now;
	        result = func.apply(context, args);
	        if (!timeout) context = args = null;
	      } else if (!timeout && options.trailing !== false) {
	        timeout = setTimeout(later, remaining);
	      }
	      return result;
	    };
	  };

	  // Returns a function, that, as long as it continues to be invoked, will not
	  // be triggered. The function will be called after it stops being called for
	  // N milliseconds. If `immediate` is passed, trigger the function on the
	  // leading edge, instead of the trailing.
	  _.debounce = function(func, wait, immediate) {
	    var timeout, args, context, timestamp, result;

	    var later = function() {
	      var last = _.now() - timestamp;

	      if (last < wait && last >= 0) {
	        timeout = setTimeout(later, wait - last);
	      } else {
	        timeout = null;
	        if (!immediate) {
	          result = func.apply(context, args);
	          if (!timeout) context = args = null;
	        }
	      }
	    };

	    return function() {
	      context = this;
	      args = arguments;
	      timestamp = _.now();
	      var callNow = immediate && !timeout;
	      if (!timeout) timeout = setTimeout(later, wait);
	      if (callNow) {
	        result = func.apply(context, args);
	        context = args = null;
	      }

	      return result;
	    };
	  };

	  // Returns the first function passed as an argument to the second,
	  // allowing you to adjust arguments, run code before and after, and
	  // conditionally execute the original function.
	  _.wrap = function(func, wrapper) {
	    return _.partial(wrapper, func);
	  };

	  // Returns a negated version of the passed-in predicate.
	  _.negate = function(predicate) {
	    return function() {
	      return !predicate.apply(this, arguments);
	    };
	  };

	  // Returns a function that is the composition of a list of functions, each
	  // consuming the return value of the function that follows.
	  _.compose = function() {
	    var args = arguments;
	    var start = args.length - 1;
	    return function() {
	      var i = start;
	      var result = args[start].apply(this, arguments);
	      while (i--) result = args[i].call(this, result);
	      return result;
	    };
	  };

	  // Returns a function that will only be executed on and after the Nth call.
	  _.after = function(times, func) {
	    return function() {
	      if (--times < 1) {
	        return func.apply(this, arguments);
	      }
	    };
	  };

	  // Returns a function that will only be executed up to (but not including) the Nth call.
	  _.before = function(times, func) {
	    var memo;
	    return function() {
	      if (--times > 0) {
	        memo = func.apply(this, arguments);
	      }
	      if (times <= 1) func = null;
	      return memo;
	    };
	  };

	  // Returns a function that will be executed at most one time, no matter how
	  // often you call it. Useful for lazy initialization.
	  _.once = _.partial(_.before, 2);

	  // Object Functions
	  // ----------------

	  // Keys in IE < 9 that won't be iterated by `for key in ...` and thus missed.
	  var hasEnumBug = !{toString: null}.propertyIsEnumerable('toString');
	  var nonEnumerableProps = ['valueOf', 'isPrototypeOf', 'toString',
	                      'propertyIsEnumerable', 'hasOwnProperty', 'toLocaleString'];

	  function collectNonEnumProps(obj, keys) {
	    var nonEnumIdx = nonEnumerableProps.length;
	    var constructor = obj.constructor;
	    var proto = (_.isFunction(constructor) && constructor.prototype) || ObjProto;

	    // Constructor is a special case.
	    var prop = 'constructor';
	    if (_.has(obj, prop) && !_.contains(keys, prop)) keys.push(prop);

	    while (nonEnumIdx--) {
	      prop = nonEnumerableProps[nonEnumIdx];
	      if (prop in obj && obj[prop] !== proto[prop] && !_.contains(keys, prop)) {
	        keys.push(prop);
	      }
	    }
	  }

	  // Retrieve the names of an object's own properties.
	  // Delegates to **ECMAScript 5**'s native `Object.keys`
	  _.keys = function(obj) {
	    if (!_.isObject(obj)) return [];
	    if (nativeKeys) return nativeKeys(obj);
	    var keys = [];
	    for (var key in obj) if (_.has(obj, key)) keys.push(key);
	    // Ahem, IE < 9.
	    if (hasEnumBug) collectNonEnumProps(obj, keys);
	    return keys;
	  };

	  // Retrieve all the property names of an object.
	  _.allKeys = function(obj) {
	    if (!_.isObject(obj)) return [];
	    var keys = [];
	    for (var key in obj) keys.push(key);
	    // Ahem, IE < 9.
	    if (hasEnumBug) collectNonEnumProps(obj, keys);
	    return keys;
	  };

	  // Retrieve the values of an object's properties.
	  _.values = function(obj) {
	    var keys = _.keys(obj);
	    var length = keys.length;
	    var values = Array(length);
	    for (var i = 0; i < length; i++) {
	      values[i] = obj[keys[i]];
	    }
	    return values;
	  };

	  // Returns the results of applying the iteratee to each element of the object
	  // In contrast to _.map it returns an object
	  _.mapObject = function(obj, iteratee, context) {
	    iteratee = cb(iteratee, context);
	    var keys =  _.keys(obj),
	          length = keys.length,
	          results = {},
	          currentKey;
	      for (var index = 0; index < length; index++) {
	        currentKey = keys[index];
	        results[currentKey] = iteratee(obj[currentKey], currentKey, obj);
	      }
	      return results;
	  };

	  // Convert an object into a list of `[key, value]` pairs.
	  _.pairs = function(obj) {
	    var keys = _.keys(obj);
	    var length = keys.length;
	    var pairs = Array(length);
	    for (var i = 0; i < length; i++) {
	      pairs[i] = [keys[i], obj[keys[i]]];
	    }
	    return pairs;
	  };

	  // Invert the keys and values of an object. The values must be serializable.
	  _.invert = function(obj) {
	    var result = {};
	    var keys = _.keys(obj);
	    for (var i = 0, length = keys.length; i < length; i++) {
	      result[obj[keys[i]]] = keys[i];
	    }
	    return result;
	  };

	  // Return a sorted list of the function names available on the object.
	  // Aliased as `methods`
	  _.functions = _.methods = function(obj) {
	    var names = [];
	    for (var key in obj) {
	      if (_.isFunction(obj[key])) names.push(key);
	    }
	    return names.sort();
	  };

	  // Extend a given object with all the properties in passed-in object(s).
	  _.extend = createAssigner(_.allKeys);

	  // Assigns a given object with all the own properties in the passed-in object(s)
	  // (https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Object/assign)
	  _.extendOwn = _.assign = createAssigner(_.keys);

	  // Returns the first key on an object that passes a predicate test
	  _.findKey = function(obj, predicate, context) {
	    predicate = cb(predicate, context);
	    var keys = _.keys(obj), key;
	    for (var i = 0, length = keys.length; i < length; i++) {
	      key = keys[i];
	      if (predicate(obj[key], key, obj)) return key;
	    }
	  };

	  // Return a copy of the object only containing the whitelisted properties.
	  _.pick = function(object, oiteratee, context) {
	    var result = {}, obj = object, iteratee, keys;
	    if (obj == null) return result;
	    if (_.isFunction(oiteratee)) {
	      keys = _.allKeys(obj);
	      iteratee = optimizeCb(oiteratee, context);
	    } else {
	      keys = flatten(arguments, false, false, 1);
	      iteratee = function(value, key, obj) { return key in obj; };
	      obj = Object(obj);
	    }
	    for (var i = 0, length = keys.length; i < length; i++) {
	      var key = keys[i];
	      var value = obj[key];
	      if (iteratee(value, key, obj)) result[key] = value;
	    }
	    return result;
	  };

	   // Return a copy of the object without the blacklisted properties.
	  _.omit = function(obj, iteratee, context) {
	    if (_.isFunction(iteratee)) {
	      iteratee = _.negate(iteratee);
	    } else {
	      var keys = _.map(flatten(arguments, false, false, 1), String);
	      iteratee = function(value, key) {
	        return !_.contains(keys, key);
	      };
	    }
	    return _.pick(obj, iteratee, context);
	  };

	  // Fill in a given object with default properties.
	  _.defaults = createAssigner(_.allKeys, true);

	  // Creates an object that inherits from the given prototype object.
	  // If additional properties are provided then they will be added to the
	  // created object.
	  _.create = function(prototype, props) {
	    var result = baseCreate(prototype);
	    if (props) _.extendOwn(result, props);
	    return result;
	  };

	  // Create a (shallow-cloned) duplicate of an object.
	  _.clone = function(obj) {
	    if (!_.isObject(obj)) return obj;
	    return _.isArray(obj) ? obj.slice() : _.extend({}, obj);
	  };

	  // Invokes interceptor with the obj, and then returns obj.
	  // The primary purpose of this method is to "tap into" a method chain, in
	  // order to perform operations on intermediate results within the chain.
	  _.tap = function(obj, interceptor) {
	    interceptor(obj);
	    return obj;
	  };

	  // Returns whether an object has a given set of `key:value` pairs.
	  _.isMatch = function(object, attrs) {
	    var keys = _.keys(attrs), length = keys.length;
	    if (object == null) return !length;
	    var obj = Object(object);
	    for (var i = 0; i < length; i++) {
	      var key = keys[i];
	      if (attrs[key] !== obj[key] || !(key in obj)) return false;
	    }
	    return true;
	  };


	  // Internal recursive comparison function for `isEqual`.
	  var eq = function(a, b, aStack, bStack) {
	    // Identical objects are equal. `0 === -0`, but they aren't identical.
	    // See the [Harmony `egal` proposal](http://wiki.ecmascript.org/doku.php?id=harmony:egal).
	    if (a === b) return a !== 0 || 1 / a === 1 / b;
	    // A strict comparison is necessary because `null == undefined`.
	    if (a == null || b == null) return a === b;
	    // Unwrap any wrapped objects.
	    if (a instanceof _) a = a._wrapped;
	    if (b instanceof _) b = b._wrapped;
	    // Compare `[[Class]]` names.
	    var className = toString.call(a);
	    if (className !== toString.call(b)) return false;
	    switch (className) {
	      // Strings, numbers, regular expressions, dates, and booleans are compared by value.
	      case '[object RegExp]':
	      // RegExps are coerced to strings for comparison (Note: '' + /a/i === '/a/i')
	      case '[object String]':
	        // Primitives and their corresponding object wrappers are equivalent; thus, `"5"` is
	        // equivalent to `new String("5")`.
	        return '' + a === '' + b;
	      case '[object Number]':
	        // `NaN`s are equivalent, but non-reflexive.
	        // Object(NaN) is equivalent to NaN
	        if (+a !== +a) return +b !== +b;
	        // An `egal` comparison is performed for other numeric values.
	        return +a === 0 ? 1 / +a === 1 / b : +a === +b;
	      case '[object Date]':
	      case '[object Boolean]':
	        // Coerce dates and booleans to numeric primitive values. Dates are compared by their
	        // millisecond representations. Note that invalid dates with millisecond representations
	        // of `NaN` are not equivalent.
	        return +a === +b;
	    }

	    var areArrays = className === '[object Array]';
	    if (!areArrays) {
	      if (typeof a != 'object' || typeof b != 'object') return false;

	      // Objects with different constructors are not equivalent, but `Object`s or `Array`s
	      // from different frames are.
	      var aCtor = a.constructor, bCtor = b.constructor;
	      if (aCtor !== bCtor && !(_.isFunction(aCtor) && aCtor instanceof aCtor &&
	                               _.isFunction(bCtor) && bCtor instanceof bCtor)
	                          && ('constructor' in a && 'constructor' in b)) {
	        return false;
	      }
	    }
	    // Assume equality for cyclic structures. The algorithm for detecting cyclic
	    // structures is adapted from ES 5.1 section 15.12.3, abstract operation `JO`.

	    // Initializing stack of traversed objects.
	    // It's done here since we only need them for objects and arrays comparison.
	    aStack = aStack || [];
	    bStack = bStack || [];
	    var length = aStack.length;
	    while (length--) {
	      // Linear search. Performance is inversely proportional to the number of
	      // unique nested structures.
	      if (aStack[length] === a) return bStack[length] === b;
	    }

	    // Add the first object to the stack of traversed objects.
	    aStack.push(a);
	    bStack.push(b);

	    // Recursively compare objects and arrays.
	    if (areArrays) {
	      // Compare array lengths to determine if a deep comparison is necessary.
	      length = a.length;
	      if (length !== b.length) return false;
	      // Deep compare the contents, ignoring non-numeric properties.
	      while (length--) {
	        if (!eq(a[length], b[length], aStack, bStack)) return false;
	      }
	    } else {
	      // Deep compare objects.
	      var keys = _.keys(a), key;
	      length = keys.length;
	      // Ensure that both objects contain the same number of properties before comparing deep equality.
	      if (_.keys(b).length !== length) return false;
	      while (length--) {
	        // Deep compare each member
	        key = keys[length];
	        if (!(_.has(b, key) && eq(a[key], b[key], aStack, bStack))) return false;
	      }
	    }
	    // Remove the first object from the stack of traversed objects.
	    aStack.pop();
	    bStack.pop();
	    return true;
	  };

	  // Perform a deep comparison to check if two objects are equal.
	  _.isEqual = function(a, b) {
	    return eq(a, b);
	  };

	  // Is a given array, string, or object empty?
	  // An "empty" object has no enumerable own-properties.
	  _.isEmpty = function(obj) {
	    if (obj == null) return true;
	    if (isArrayLike(obj) && (_.isArray(obj) || _.isString(obj) || _.isArguments(obj))) return obj.length === 0;
	    return _.keys(obj).length === 0;
	  };

	  // Is a given value a DOM element?
	  _.isElement = function(obj) {
	    return !!(obj && obj.nodeType === 1);
	  };

	  // Is a given value an array?
	  // Delegates to ECMA5's native Array.isArray
	  _.isArray = nativeIsArray || function(obj) {
	    return toString.call(obj) === '[object Array]';
	  };

	  // Is a given variable an object?
	  _.isObject = function(obj) {
	    var type = typeof obj;
	    return type === 'function' || type === 'object' && !!obj;
	  };

	  // Add some isType methods: isArguments, isFunction, isString, isNumber, isDate, isRegExp, isError.
	  _.each(['Arguments', 'Function', 'String', 'Number', 'Date', 'RegExp', 'Error'], function(name) {
	    _['is' + name] = function(obj) {
	      return toString.call(obj) === '[object ' + name + ']';
	    };
	  });

	  // Define a fallback version of the method in browsers (ahem, IE < 9), where
	  // there isn't any inspectable "Arguments" type.
	  if (!_.isArguments(arguments)) {
	    _.isArguments = function(obj) {
	      return _.has(obj, 'callee');
	    };
	  }

	  // Optimize `isFunction` if appropriate. Work around some typeof bugs in old v8,
	  // IE 11 (#1621), and in Safari 8 (#1929).
	  if (typeof /./ != 'function' && typeof Int8Array != 'object') {
	    _.isFunction = function(obj) {
	      return typeof obj == 'function' || false;
	    };
	  }

	  // Is a given object a finite number?
	  _.isFinite = function(obj) {
	    return isFinite(obj) && !isNaN(parseFloat(obj));
	  };

	  // Is the given value `NaN`? (NaN is the only number which does not equal itself).
	  _.isNaN = function(obj) {
	    return _.isNumber(obj) && obj !== +obj;
	  };

	  // Is a given value a boolean?
	  _.isBoolean = function(obj) {
	    return obj === true || obj === false || toString.call(obj) === '[object Boolean]';
	  };

	  // Is a given value equal to null?
	  _.isNull = function(obj) {
	    return obj === null;
	  };

	  // Is a given variable undefined?
	  _.isUndefined = function(obj) {
	    return obj === void 0;
	  };

	  // Shortcut function for checking if an object has a given property directly
	  // on itself (in other words, not on a prototype).
	  _.has = function(obj, key) {
	    return obj != null && hasOwnProperty.call(obj, key);
	  };

	  // Utility Functions
	  // -----------------

	  // Run Underscore.js in *noConflict* mode, returning the `_` variable to its
	  // previous owner. Returns a reference to the Underscore object.
	  _.noConflict = function() {
	    root._ = previousUnderscore;
	    return this;
	  };

	  // Keep the identity function around for default iteratees.
	  _.identity = function(value) {
	    return value;
	  };

	  // Predicate-generating functions. Often useful outside of Underscore.
	  _.constant = function(value) {
	    return function() {
	      return value;
	    };
	  };

	  _.noop = function(){};

	  _.property = property;

	  // Generates a function for a given object that returns a given property.
	  _.propertyOf = function(obj) {
	    return obj == null ? function(){} : function(key) {
	      return obj[key];
	    };
	  };

	  // Returns a predicate for checking whether an object has a given set of
	  // `key:value` pairs.
	  _.matcher = _.matches = function(attrs) {
	    attrs = _.extendOwn({}, attrs);
	    return function(obj) {
	      return _.isMatch(obj, attrs);
	    };
	  };

	  // Run a function **n** times.
	  _.times = function(n, iteratee, context) {
	    var accum = Array(Math.max(0, n));
	    iteratee = optimizeCb(iteratee, context, 1);
	    for (var i = 0; i < n; i++) accum[i] = iteratee(i);
	    return accum;
	  };

	  // Return a random integer between min and max (inclusive).
	  _.random = function(min, max) {
	    if (max == null) {
	      max = min;
	      min = 0;
	    }
	    return min + Math.floor(Math.random() * (max - min + 1));
	  };

	  // A (possibly faster) way to get the current timestamp as an integer.
	  _.now = Date.now || function() {
	    return new Date().getTime();
	  };

	   // List of HTML entities for escaping.
	  var escapeMap = {
	    '&': '&amp;',
	    '<': '&lt;',
	    '>': '&gt;',
	    '"': '&quot;',
	    "'": '&#x27;',
	    '`': '&#x60;'
	  };
	  var unescapeMap = _.invert(escapeMap);

	  // Functions for escaping and unescaping strings to/from HTML interpolation.
	  var createEscaper = function(map) {
	    var escaper = function(match) {
	      return map[match];
	    };
	    // Regexes for identifying a key that needs to be escaped
	    var source = '(?:' + _.keys(map).join('|') + ')';
	    var testRegexp = RegExp(source);
	    var replaceRegexp = RegExp(source, 'g');
	    return function(string) {
	      string = string == null ? '' : '' + string;
	      return testRegexp.test(string) ? string.replace(replaceRegexp, escaper) : string;
	    };
	  };
	  _.escape = createEscaper(escapeMap);
	  _.unescape = createEscaper(unescapeMap);

	  // If the value of the named `property` is a function then invoke it with the
	  // `object` as context; otherwise, return it.
	  _.result = function(object, property, fallback) {
	    var value = object == null ? void 0 : object[property];
	    if (value === void 0) {
	      value = fallback;
	    }
	    return _.isFunction(value) ? value.call(object) : value;
	  };

	  // Generate a unique integer id (unique within the entire client session).
	  // Useful for temporary DOM ids.
	  var idCounter = 0;
	  _.uniqueId = function(prefix) {
	    var id = ++idCounter + '';
	    return prefix ? prefix + id : id;
	  };

	  // By default, Underscore uses ERB-style template delimiters, change the
	  // following template settings to use alternative delimiters.
	  _.templateSettings = {
	    evaluate    : /<%([\s\S]+?)%>/g,
	    interpolate : /<%=([\s\S]+?)%>/g,
	    escape      : /<%-([\s\S]+?)%>/g
	  };

	  // When customizing `templateSettings`, if you don't want to define an
	  // interpolation, evaluation or escaping regex, we need one that is
	  // guaranteed not to match.
	  var noMatch = /(.)^/;

	  // Certain characters need to be escaped so that they can be put into a
	  // string literal.
	  var escapes = {
	    "'":      "'",
	    '\\':     '\\',
	    '\r':     'r',
	    '\n':     'n',
	    '\u2028': 'u2028',
	    '\u2029': 'u2029'
	  };

	  var escaper = /\\|'|\r|\n|\u2028|\u2029/g;

	  var escapeChar = function(match) {
	    return '\\' + escapes[match];
	  };

	  // JavaScript micro-templating, similar to John Resig's implementation.
	  // Underscore templating handles arbitrary delimiters, preserves whitespace,
	  // and correctly escapes quotes within interpolated code.
	  // NB: `oldSettings` only exists for backwards compatibility.
	  _.template = function(text, settings, oldSettings) {
	    if (!settings && oldSettings) settings = oldSettings;
	    settings = _.defaults({}, settings, _.templateSettings);

	    // Combine delimiters into one regular expression via alternation.
	    var matcher = RegExp([
	      (settings.escape || noMatch).source,
	      (settings.interpolate || noMatch).source,
	      (settings.evaluate || noMatch).source
	    ].join('|') + '|$', 'g');

	    // Compile the template source, escaping string literals appropriately.
	    var index = 0;
	    var source = "__p+='";
	    text.replace(matcher, function(match, escape, interpolate, evaluate, offset) {
	      source += text.slice(index, offset).replace(escaper, escapeChar);
	      index = offset + match.length;

	      if (escape) {
	        source += "'+\n((__t=(" + escape + "))==null?'':_.escape(__t))+\n'";
	      } else if (interpolate) {
	        source += "'+\n((__t=(" + interpolate + "))==null?'':__t)+\n'";
	      } else if (evaluate) {
	        source += "';\n" + evaluate + "\n__p+='";
	      }

	      // Adobe VMs need the match returned to produce the correct offest.
	      return match;
	    });
	    source += "';\n";

	    // If a variable is not specified, place data values in local scope.
	    if (!settings.variable) source = 'with(obj||{}){\n' + source + '}\n';

	    source = "var __t,__p='',__j=Array.prototype.join," +
	      "print=function(){__p+=__j.call(arguments,'');};\n" +
	      source + 'return __p;\n';

	    try {
	      var render = new Function(settings.variable || 'obj', '_', source);
	    } catch (e) {
	      e.source = source;
	      throw e;
	    }

	    var template = function(data) {
	      return render.call(this, data, _);
	    };

	    // Provide the compiled source as a convenience for precompilation.
	    var argument = settings.variable || 'obj';
	    template.source = 'function(' + argument + '){\n' + source + '}';

	    return template;
	  };

	  // Add a "chain" function. Start chaining a wrapped Underscore object.
	  _.chain = function(obj) {
	    var instance = _(obj);
	    instance._chain = true;
	    return instance;
	  };

	  // OOP
	  // ---------------
	  // If Underscore is called as a function, it returns a wrapped object that
	  // can be used OO-style. This wrapper holds altered versions of all the
	  // underscore functions. Wrapped objects may be chained.

	  // Helper function to continue chaining intermediate results.
	  var result = function(instance, obj) {
	    return instance._chain ? _(obj).chain() : obj;
	  };

	  // Add your own custom functions to the Underscore object.
	  _.mixin = function(obj) {
	    _.each(_.functions(obj), function(name) {
	      var func = _[name] = obj[name];
	      _.prototype[name] = function() {
	        var args = [this._wrapped];
	        push.apply(args, arguments);
	        return result(this, func.apply(_, args));
	      };
	    });
	  };

	  // Add all of the Underscore functions to the wrapper object.
	  _.mixin(_);

	  // Add all mutator Array functions to the wrapper.
	  _.each(['pop', 'push', 'reverse', 'shift', 'sort', 'splice', 'unshift'], function(name) {
	    var method = ArrayProto[name];
	    _.prototype[name] = function() {
	      var obj = this._wrapped;
	      method.apply(obj, arguments);
	      if ((name === 'shift' || name === 'splice') && obj.length === 0) delete obj[0];
	      return result(this, obj);
	    };
	  });

	  // Add all accessor Array functions to the wrapper.
	  _.each(['concat', 'join', 'slice'], function(name) {
	    var method = ArrayProto[name];
	    _.prototype[name] = function() {
	      return result(this, method.apply(this._wrapped, arguments));
	    };
	  });

	  // Extracts the result from a wrapped and chained object.
	  _.prototype.value = function() {
	    return this._wrapped;
	  };

	  // Provide unwrapping proxy for some methods used in engine operations
	  // such as arithmetic and JSON stringification.
	  _.prototype.valueOf = _.prototype.toJSON = _.prototype.value;

	  _.prototype.toString = function() {
	    return '' + this._wrapped;
	  };

	  // AMD registration happens at the end for compatibility with AMD loaders
	  // that may not enforce next-turn semantics on modules. Even though general
	  // practice for AMD registration is to be anonymous, underscore registers
	  // as a named module because, like jQuery, it is a base library that is
	  // popular enough to be bundled in a third party lib, but not be part of
	  // an AMD load request. Those cases could generate an error when an
	  // anonymous define() is called outside of a loader request.
	  if (true) {
	    !(__WEBPACK_AMD_DEFINE_ARRAY__ = [], __WEBPACK_AMD_DEFINE_RESULT__ = function() {
	      return _;
	    }.apply(exports, __WEBPACK_AMD_DEFINE_ARRAY__), __WEBPACK_AMD_DEFINE_RESULT__ !== undefined && (module.exports = __WEBPACK_AMD_DEFINE_RESULT__));
	  }
	}.call(this));


/***/ }),
/* 123 */
/***/ (function(module, exports) {

	"use strict"

	function compileSearch(funcName, predicate, reversed, extraArgs, earlyOut) {
	  var code = [
	    "function ", funcName, "(a,l,h,", extraArgs.join(","),  "){",
	earlyOut ? "" : "var i=", (reversed ? "l-1" : "h+1"),
	";while(l<=h){\
	var m=(l+h)>>>1,x=a[m]"]
	  if(earlyOut) {
	    if(predicate.indexOf("c") < 0) {
	      code.push(";if(x===y){return m}else if(x<=y){")
	    } else {
	      code.push(";var p=c(x,y);if(p===0){return m}else if(p<=0){")
	    }
	  } else {
	    code.push(";if(", predicate, "){i=m;")
	  }
	  if(reversed) {
	    code.push("l=m+1}else{h=m-1}")
	  } else {
	    code.push("h=m-1}else{l=m+1}")
	  }
	  code.push("}")
	  if(earlyOut) {
	    code.push("return -1};")
	  } else {
	    code.push("return i};")
	  }
	  return code.join("")
	}

	function compileBoundsSearch(predicate, reversed, suffix, earlyOut) {
	  var result = new Function([
	  compileSearch("A", "x" + predicate + "y", reversed, ["y"], earlyOut),
	  compileSearch("P", "c(x,y)" + predicate + "0", reversed, ["y", "c"], earlyOut),
	"function dispatchBsearch", suffix, "(a,y,c,l,h){\
	if(typeof(c)==='function'){\
	return P(a,(l===void 0)?0:l|0,(h===void 0)?a.length-1:h|0,y,c)\
	}else{\
	return A(a,(c===void 0)?0:c|0,(l===void 0)?a.length-1:l|0,y)\
	}}\
	return dispatchBsearch", suffix].join(""))
	  return result()
	}

	module.exports = {
	  ge: compileBoundsSearch(">=", false, "GE"),
	  gt: compileBoundsSearch(">", false, "GT"),
	  lt: compileBoundsSearch("<", true, "LT"),
	  le: compileBoundsSearch("<=", true, "LE"),
	  eq: compileBoundsSearch("-", true, "EQ", true)
	}


/***/ }),
/* 124 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , InMemoryCollectionRoutingMap = __webpack_require__(121)
	    , semaphore = __webpack_require__(72);

	var CollectionRoutingMapFactory = InMemoryCollectionRoutingMap.CollectionRoutingMapFactory;

	//SCRIPT START
	var PartitionKeyRangeCache = Base.defineClass(
	    
	    /**
	     * Represents a PartitionKeyRangeCache. PartitionKeyRangeCache provides list of effective partition key ranges for a collection.
	     * This implementation loads and caches the collection routing map per collection on demand.
	     * @constructor PartitionKeyRangeCache
	     * @param {object} documentclient                - The documentclient object.
	     * @ignore
	     */
	    function (documentclient) {
	        this.documentclient = documentclient;
	        this.collectionRoutingMapByCollectionId = {};
	        this.sem = semaphore(1);
	    },
	    {
	        /**
	         * Finds or Instantiates the requested Collection Routing Map and invokes callback
	         * @param {callback} callback                - Function to execute for the collection routing map. the function takes two parameters error, collectionRoutingMap.
	         * @param {string} collectionLink            - Requested collectionLink
	         * @ignore
	         */
	        _onCollectionRoutingMap: function (callback, collectionLink) {
	            var isNameBased = Base.isLinkNameBased(collectionLink);
	            var collectionId = this.documentclient.getIdFromLink(collectionLink, isNameBased);

	            var collectionRoutingMap = this.collectionRoutingMapByCollectionId[collectionId];
	            if (collectionRoutingMap === undefined) {
	                // attempt to consturct collection routing map
	                var that = this;
	                var semaphorizedFuncCollectionMapInstantiator = function () {
	                    var collectionRoutingMap = that.collectionRoutingMapByCollectionId[collectionId];
	                    if (collectionRoutingMap === undefined) {
	                        var partitionKeyRangesIterator = that.documentclient.readPartitionKeyRanges(collectionLink);
	                        partitionKeyRangesIterator.toArray(function (err, resources) {
	                            if (err) {
	                                return callback(err, undefined);
	                            }

	                            collectionRoutingMap = CollectionRoutingMapFactory.createCompleteRoutingMap(
	                                resources.map(function (r) { return [r, true]; }),
	                                collectionId);

	                            that.collectionRoutingMapByCollectionId[collectionId] = collectionRoutingMap;
	                            that.sem.leave();
	                            return callback(undefined, collectionRoutingMap);
	                        });

	                    } else {
	                        // sanity gaurd 
	                        that.sem.leave();
	                        return callback(undefined, collectionRoutingMap.getOverlappingRanges(partitionKeyRanges));
	                    }
	                };

	                // We want only one attempt to construct collectionRoutingMap so we pass the consturction in the semaphore take
	                this.sem.take(semaphorizedFuncCollectionMapInstantiator);

	            } else {
	                callback(undefined, collectionRoutingMap);
	            }
	        }, 

	        /**
	         * Given the query ranges and a collection, invokes the callback on the list of overlapping partition key ranges
	         * @param {callback} callback - Function execute on the overlapping partition key ranges result, takes two parameters error, partition key ranges
	         * @param collectionLink
	         * @param queryRanges
	         * @ignore
	         */
	        getOverlappingRanges: function (callback, collectionLink, queryRanges) {
	            this._onCollectionRoutingMap(function (err, collectionRoutingMap) {
	                if (err) {
	                    return callback(err, undefined);
	                }
	                return callback(undefined, collectionRoutingMap.getOverlappingRanges(queryRanges));
	            }, collectionLink);
	        }
	    }
	);
	//SCRIPT END

	if (true) {
	    module.exports = PartitionKeyRangeCache;
	}

/***/ }),
/* 125 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , ParallelQueryExecutionContextBase = __webpack_require__(118)
	    , OrderByDocumentProducerComparator = __webpack_require__(115)
	    , assert = __webpack_require__(103);

	//SCRIPT START

	var OrderByQueryExecutionContext = Base.derive(
	    ParallelQueryExecutionContextBase,
	    /**
	     * Provides the OrderByQueryExecutionContext.
	     * This class is capable of handling orderby queries and dervives from ParallelQueryExecutionContextBase.
	     *
	     * When handling a parallelized query, it instantiates one instance of
	     * DocumentProcuder per target partition key range and aggregates the result of each.
	     *
	     * @constructor ParallelQueryExecutionContext
	     * @param {DocumentClient} documentclient        - The service endpoint to use to create the client.
	     * @param {string} collectionLink                - The Collection Link
	     * @param {FeedOptions} [options]                - Represents the feed options.
	     * @param {object} partitionedQueryExecutionInfo - PartitionedQueryExecutionInfo
	     * @ignore
	     */
	    function (documentclient, collectionLink, query, options, partitionedQueryExecutionInfo) {
	        // Calling on base class constructor
	        ParallelQueryExecutionContextBase.call(this, documentclient, collectionLink, query, options, partitionedQueryExecutionInfo);
	        this._orderByComparator = new OrderByDocumentProducerComparator(this.sortOrders);
	    },
	    {
	        // Instance members are inherited
	        
	        // Overriding documentProducerComparator for OrderByQueryExecutionContexts
	        /**
	         * Provides a Comparator for document producers which respects orderby sort order.
	         * @returns {object}        - Comparator Function
	         * @ignore
	         */
	        documentProducerComparator: function (docProd1, docProd2) {
	            return this._orderByComparator.compare(docProd1, docProd2);
	        },
	    },
	    {
	        // Static members are inherited
	    }
	);

	//SCRIPT END

	if (true) {
	    module.exports = OrderByQueryExecutionContext;
	}

/***/ }),
/* 126 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Documents = __webpack_require__(104)
	    , Constants = __webpack_require__(69)
	    , https = __webpack_require__(73)
	    , url = __webpack_require__(94)
	    , querystring = __webpack_require__(98)
	    , RetryUtility = __webpack_require__(127);

	//----------------------------------------------------------------------------
	// Utility methods
	//

	function javaScriptFriendlyJSONStringify(s) {
	    // two line terminators (Line separator and Paragraph separator) are not needed to be escaped in JSON
	    // but are needed to be escaped in JavaScript.
	    return JSON.stringify(s).
	        replace(/\u2028/g, '\\u2028').
	        replace(/\u2029/g, '\\u2029');
	}

	function bodyFromData(data) {
	    if (data.pipe) return data;
	    if (Buffer.isBuffer(data)) return data;
	    if (typeof data === "string") return data;
	    if (typeof data === "object") return javaScriptFriendlyJSONStringify(data);
	    return undefined;
	}

	function parse(urlString) { return url.parse(urlString); }

	function createRequestObject(connectionPolicy, requestOptions, callback) {
	    function onTimeout() {
	        httpsRequest.abort();
	    }

	    var isMedia = (requestOptions.path.indexOf("//media") === 0);

	    var httpsRequest = https.request(requestOptions, function (response) {
	        // In case of media response, return the stream to the user and the user will need to handle reading the stream.
	        if (isMedia && connectionPolicy.MediaReadMode === Documents.MediaReadMode.Streamed) {
	            return callback(undefined, response, response.headers);
	        }

	        var data = "";

	        //if the requested data is text (not attachment/media) set the encoding to UTF-8
	        if (!isMedia) {
	            response.setEncoding("utf8");
	        }

	        response.on("data", function (chunk) {
	            data += chunk;
	        });
	        response.on("end", function () {
	            if (response.statusCode >= 400) {
	                return callback(getErrorBody(response, data), undefined, response.headers);
	            }

	            var result;
	            try {
	                if (isMedia) {
	                    result = data;
	                } else {
	                    result = data.length > 0 ? JSON.parse(data) : undefined;
	                }
	            } catch (exception) {
	                return callback(exception);
	            }

	            callback(undefined, result, response.headers);
	        });
	    });

	    httpsRequest.once("socket", function (socket) {
	        if (isMedia) {
	            socket.setTimeout(connectionPolicy.MediaRequestTimeout);
	        } else {
	            socket.setTimeout(connectionPolicy.RequestTimeout);
	        }

	        socket.once("timeout", onTimeout);

	        httpsRequest.once("response", function () {
	            socket.removeListener("timeout", onTimeout);
	        });
	    });

	    httpsRequest.once("error", callback);
	    return httpsRequest;
	}

	/**
	*  Constructs the error body from the response and the data returned from the request.
	* @param {object} response - response object returned from the executon of a request.
	* @param {object} data - the data body returned from the executon of a request.
	*/
	function getErrorBody(response, data) {
	    var errorBody = { code: response.statusCode, body: data };

	    if (Constants.HttpHeaders.ActivityId in response.headers) {
	        errorBody.activityId = response.headers[Constants.HttpHeaders.ActivityId];
	    }

	    if (Constants.HttpHeaders.SubStatus in response.headers) {
	        errorBody.substatus = parseInt(response.headers[Constants.HttpHeaders.SubStatus]);
	    }

	    if (Constants.HttpHeaders.RetryAfterInMilliseconds in response.headers) {
	        errorBody.retryAfterInMilliseconds = parseInt(response.headers[Constants.HttpHeaders.RetryAfterInMilliseconds]);
	    }

	    return errorBody;
	}

	var RequestHandler = {
	    _createRequestObjectStub: function (connectionPolicy, requestOptions, callback) {
	        return createRequestObject(connectionPolicy, requestOptions, callback);
	    },

	    /**
	     *  Creates the request object, call the passed callback when the response is retrieved.
	     * @param {object} globalEndpointManager - an instance of GlobalEndpointManager class.
	     * @param {object} connectionPolicy - an instance of ConnectionPolicy that has the connection configs.
	     * @param {object} requestAgent - the https agent used for send request
	     * @param {string} method - the http request method ( 'get', 'post', 'put', .. etc ).
	     * @param {String} url - The base url for the endpoint.
	     * @param {string} path - the path of the requesed resource.
	     * @param {Object} data - the request body. It can be either string, buffer, stream or undefined.
	     * @param {Object} queryParams - query parameters for the request.
	     * @param {Object} headers - specific headers for the request.
	     * @param {function} callback - the callback that will be called when the response is retrieved and processed.
	    */
	    request: function (globalEndpointManager, connectionPolicy, requestAgent, method, url, request, data, queryParams, headers, callback) {
	        var path = request.path == undefined ? request : request.path;
	        var body;

	        if (data) {
	            body = bodyFromData(data);
	            if (!body) return callback({ message: "parameter data must be a javascript object, string, Buffer, or stream" });
	        }

	        var buffer;
	        var stream;
	        if (body) {
	            if (Buffer.isBuffer(body)) {
	                buffer = body;
	            } else if (body.pipe) {
	                // it is a stream
	                stream = body;
	            } else if (typeof body === "string") {
	                buffer = new Buffer(body, "utf8");
	            } else {
	                return callback({ message: "body must be string, Buffer, or stream" });
	            }
	        }

	        var requestOptions = parse(url);
	        requestOptions.method = method;
	        requestOptions.path = path;
	        requestOptions.headers = headers;
	        requestOptions.agent = requestAgent;
	        requestOptions.secureProtocol = "TLSv1_client_method";

	        if (connectionPolicy.DisableSSLVerification === true) {
	            requestOptions.rejectUnauthorized = false;
	        }

	        if (queryParams) {
	            requestOptions.path += "?" + querystring.stringify(queryParams);
	        }

	        if (buffer) {
	            requestOptions.headers[Constants.HttpHeaders.ContentLength] = buffer.length;
	            RetryUtility.execute(globalEndpointManager, { buffer: buffer, stream: null }, this._createRequestObjectStub, connectionPolicy, requestOptions, request, callback);
	        } else if (stream) {
	            RetryUtility.execute(globalEndpointManager, { buffer: null, stream: stream }, this._createRequestObjectStub, connectionPolicy, requestOptions, request, callback);
	        } else {
	            RetryUtility.execute(globalEndpointManager, { buffer: null, stream: null }, this._createRequestObjectStub, connectionPolicy, requestOptions, request, callback);
	        }
	    }
	}

	if (true) {
	    module.exports = RequestHandler;
	}

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 127 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4),
	    Constants = __webpack_require__(69),
	    EndpointDiscoveryRetryPolicy = __webpack_require__(128),
	    ResourceThrottleRetryPolicy = __webpack_require__(129),
	    SessionReadRetryPolicy = __webpack_require__(130);

	//SCRIPT START
	var RetryUtility = {
	    /**
	    * Executes the retry policy for the created request object.
	    * @param {object} globalEndpointManager - an instance of GlobalEndpointManager class.
	    * @param {object} body - a dictionary containing 'buffer' and 'stream' keys to hold corresponding buffer or stream body, null otherwise.
	    * @param {function} createRequestObjectStub - stub function that creates the request object.
	    * @param {object} connectionPolicy - an instance of ConnectionPolicy that has the connection configs.
	    * @param {RequestOptions} requestOptions - The request options.
	    * @param {function} callback - the callback that will be called when the request is finished executing.
	    */
	    execute: function (globalEndpointManager, body, createRequestObjectFunc, connectionPolicy, requestOptions, request, callback) {
	        var request = typeof request !== 'string' ? request : { "path": "", "operationType": "nonReadOps", "client": null };

	        var endpointDiscoveryRetryPolicy = new EndpointDiscoveryRetryPolicy(globalEndpointManager);
	        var resourceThrottleRetryPolicy = new ResourceThrottleRetryPolicy(connectionPolicy.RetryOptions.MaxRetryAttemptCount,
	            connectionPolicy.RetryOptions.FixedRetryIntervalInMilliseconds,
	            connectionPolicy.RetryOptions.MaxWaitTimeInSeconds);
	        var sessionReadRetryPolicy = new SessionReadRetryPolicy(globalEndpointManager, request)

	        this.apply(body, createRequestObjectFunc, connectionPolicy, requestOptions, endpointDiscoveryRetryPolicy, resourceThrottleRetryPolicy, sessionReadRetryPolicy, callback);
	    },

	    /**
	    * Applies the retry policy for the created request object.
	    * @param {object} body - a dictionary containing 'buffer' and 'stream' keys to hold corresponding buffer or stream body, null otherwise.
	    * @param {function} createRequestObjectFunc - function that creates the request object.
	    * @param {object} connectionPolicy - an instance of ConnectionPolicy that has the connection configs.
	    * @param {RequestOptions} requestOptions - The request options.
	    * @param {EndpointDiscoveryRetryPolicy} endpointDiscoveryRetryPolicy - The endpoint discovery retry policy instance.
	    * @param {ResourceThrottleRetryPolicy} resourceThrottleRetryPolicy - The resource throttle retry policy instance.
	    * @param {function} callback - the callback that will be called when the response is retrieved and processed.
	    */
	    apply: function (body, createRequestObjectFunc, connectionPolicy, requestOptions, endpointDiscoveryRetryPolicy, resourceThrottleRetryPolicy, sessionReadRetryPolicy, callback) {
	        var that = this;
	        var httpsRequest = createRequestObjectFunc(connectionPolicy, requestOptions, function (err, response, headers) {
	            if (err) {
	                var retryPolicy = null;
	                headers = headers || {};
	                if (err.code === EndpointDiscoveryRetryPolicy.FORBIDDEN_STATUS_CODE && err.substatus === EndpointDiscoveryRetryPolicy.WRITE_FORBIDDEN_SUB_STATUS_CODE) {
	                    retryPolicy = endpointDiscoveryRetryPolicy;
	                } else if (err.code === ResourceThrottleRetryPolicy.THROTTLE_STATUS_CODE) {
	                    retryPolicy = resourceThrottleRetryPolicy;
	                } else if (err.code === SessionReadRetryPolicy.NOT_FOUND_STATUS_CODE && err.substatus === SessionReadRetryPolicy.READ_SESSION_NOT_AVAILABLE_SUB_STATUS_CODE) {
	                    retryPolicy = sessionReadRetryPolicy;
	                }
	                if (retryPolicy) {
	                    retryPolicy.shouldRetry(err, function (shouldRetry, newUrl) {
	                        if (!shouldRetry) {
	                            headers[Constants.ThrottleRetryCount] = resourceThrottleRetryPolicy.currentRetryAttemptCount;
	                            headers[Constants.ThrottleRetryWaitTimeInMs] = resourceThrottleRetryPolicy.cummulativeWaitTimeinMilliseconds;
	                            return callback(err, response, headers);
	                        } else {
	                            setTimeout(function () {
	                                if (typeof newUrl !== 'undefined')
	                                    requestOptions = that.modifyRequestOptions(requestOptions, newUrl);
	                                that.apply(body, createRequestObjectFunc, connectionPolicy, requestOptions, endpointDiscoveryRetryPolicy, resourceThrottleRetryPolicy, sessionReadRetryPolicy, callback);
	                            }, retryPolicy.retryAfterInMilliseconds);
	                            return;
	                        }
	                    });
	                    return;
	                }
	            }
	            headers[Constants.ThrottleRetryCount] = resourceThrottleRetryPolicy.currentRetryAttemptCount;
	            headers[Constants.ThrottleRetryWaitTimeInMs] = resourceThrottleRetryPolicy.cummulativeWaitTimeinMilliseconds;
	            return callback(err, response, headers);
	        });

	        if (httpsRequest) {
	            if (body["stream"] !== null) {
	                body["stream"].pipe(httpsRequest);
	            } else if (body["buffer"] !== null) {
	                httpsRequest.write(body["buffer"]);
	                httpsRequest.end();
	            } else {
	                httpsRequest.end();
	            }
	        }
	    },

	    modifyRequestOptions: function (oldRequestOptions, newUrl) {
	        var properties = Object.keys(newUrl);
	        for (var index in properties) {
	            if (properties[index] !== "path")
	                oldRequestOptions[properties[index]] = newUrl[properties[index]];
	        }
	        return oldRequestOptions;
	    }
	}
	//SCRIPT END

	if (true) {
	    module.exports = RetryUtility;
	}

/***/ }),
/* 128 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4);

	//SCRIPT START
	/**
	     * This class implements the retry policy for endpoint discovery.
	     * @property {int} _maxRetryAttemptCount                           - Max number of retry attempts to perform.
	     * @property {int} currentRetryAttemptCount                        - Current retry attempt count.
	     * @property {object} globalEndpointManager                        - The GlobalEndpointManager instance.
	     * @property {int} retryAfterInMilliseconds                        - Retry interval in milliseconds.
	*/
	var EndpointDiscoveryRetryPolicy = Base.defineClass(
	    /**
	     * @constructor EndpointDiscoveryRetryPolicy
	     * @param {object} globalEndpointManager                           - The GlobalEndpointManager instance.
	    */
	    function (globalEndpointManager) {
	        this._maxRetryAttemptCount = EndpointDiscoveryRetryPolicy.maxRetryAttemptCount;
	        this.currentRetryAttemptCount = 0;
	        this.globalEndpointManager = globalEndpointManager;
	        this.retryAfterInMilliseconds = EndpointDiscoveryRetryPolicy.retryAfterInMilliseconds;
	    }, 
	    {
	        /**
	         * Determines whether the request should be retried or not.
	         * @param {object} err - Error returned by the request.
	         * @param {function} callback - The callback function which takes bool argument which specifies whether the request will be retried or not.
	        */
	        shouldRetry: function (err, callback) {
	            if (err) {
	                if (this.currentRetryAttemptCount < this._maxRetryAttemptCount && this.globalEndpointManager.enableEndpointDiscovery) {
	                    this.currentRetryAttemptCount++;
	                    console.log("Write region was changed, refreshing the regions list from database account and will retry the request.");
	                    var that = this;
	                    this.globalEndpointManager.refreshEndpointList(function (writeEndpoint, readEndpoint) {
	                        that.globalEndpointManager.setWriteEndpoint(writeEndpoint);
	                        that.globalEndpointManager.setReadEndpoint(readEndpoint);
	                        callback(true);
	                    });
	                    return;
	                }
	            }
	            return callback(false);
	        }
	    },
	    {
	        maxRetryAttemptCount : 120,
	        retryAfterInMilliseconds : 1000,
	        FORBIDDEN_STATUS_CODE : 403,
	        WRITE_FORBIDDEN_SUB_STATUS_CODE : 3
	    }
	);
	//SCRIPT END

	if (true) {
	    module.exports = EndpointDiscoveryRetryPolicy;
	}

/***/ }),
/* 129 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4);

	//SCRIPT START
	/**
	     * This class implements the resource throttle retry policy for requests.
	     * @property {int} _maxRetryAttemptCount              - Max number of retries to be performed for a request.
	     * @property {int} _fixedRetryIntervalInMilliseconds  - Fixed retry interval in milliseconds to wait between each retry ignoring the retryAfter returned as part of the response. 
	     * @property {int} _maxWaitTimeInMilliseconds         - Max wait time in milliseconds to wait for a request while the retries are happening.
	     * @property {int} currentRetryAttemptCount           - Current retry attempt count.
	     * @property {int} cummulativeWaitTimeinMilliseconds  - Cummulative wait time in milliseconds for a request while the retries are happening.
	*/
	var ResourceThrottleRetryPolicy = Base.defineClass(
	    /**
	     * @constructor ResourceThrottleRetryPolicy
	     * @param {int} maxRetryAttemptCount               - Max number of retries to be performed for a request.
	     * @param {int} fixedRetryIntervalInMilliseconds   - Fixed retry interval in milliseconds to wait between each retry ignoring the retryAfter returned as part of the response.
	     * @param {int} maxWaitTimeInSeconds               - Max wait time in seconds to wait for a request while the retries are happening.
	    */
	    function (maxRetryAttemptCount, fixedRetryIntervalInMilliseconds, maxWaitTimeInSeconds) {
	        this._maxRetryAttemptCount = maxRetryAttemptCount;
	        this._fixedRetryIntervalInMilliseconds = fixedRetryIntervalInMilliseconds;
	        this._maxWaitTimeInMilliseconds = maxWaitTimeInSeconds * 1000;
	        this.currentRetryAttemptCount = 0;
	        this.cummulativeWaitTimeinMilliseconds = 0;
	    }, 
	    {
	        /**
	         * Determines whether the request should be retried or not.
	         * @param {object} err - Error returned by the request.
	         * @param {function} callback - The callback function which takes bool argument which specifies whether the request will be retried or not.
	        */
	        shouldRetry: function (err, callback) {
	            if (err) {
	                if (this.currentRetryAttemptCount < this._maxRetryAttemptCount) {
	                    this.currentRetryAttemptCount++;
	                    this.retryAfterInMilliseconds = 0;

	                    if (this._fixedRetryIntervalInMilliseconds) {
	                        this.retryAfterInMilliseconds = this._fixedRetryIntervalInMilliseconds;
	                    } else if (err.retryAfterInMilliseconds) {
	                        this.retryAfterInMilliseconds = err.retryAfterInMilliseconds;
	                    }
	    
	                    if (this.cummulativeWaitTimeinMilliseconds < this._maxWaitTimeInMilliseconds) {
	                        this.cummulativeWaitTimeinMilliseconds += this.retryAfterInMilliseconds;
	                        return callback(true);
	                    }
	                }
	            }
	            return callback(false);
	        }
	    },
	    {
	        THROTTLE_STATUS_CODE: 429
	    }
	);
	//SCRIPT END

	if (true) {
	    module.exports = ResourceThrottleRetryPolicy;
	}

/***/ }),
/* 130 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , Constants = __webpack_require__(69)
	    , url = __webpack_require__(94);

	//SCRIPT START
	/**
	     * This class implements the retry policy for session consistent reads.
	     * @property {int} _maxRetryAttemptCount                           - Max number of retry attempts to perform.
	     * @property {int} currentRetryAttemptCount                        - Current retry attempt count.
	     * @property {object} globalEndpointManager                        - The GlobalEndpointManager instance.
	     * @property {object} request                                      - The Http request information
	     * @property {int} retryAfterInMilliseconds                        - Retry interval in milliseconds.
	*/
	var SessionReadRetryPolicy = Base.defineClass(
	    /**
	     * @constructor SessionReadRetryPolicy
	     * @param {object} globalEndpointManager                           - The GlobalEndpointManager instance.
	     * @property {object} request                                      - The Http request information
	     */
	    function (globalEndpointManager, request) {
	        this._maxRetryAttemptCount = SessionReadRetryPolicy.maxRetryAttemptCount;
	        this.currentRetryAttemptCount = 0;
	        this.globalEndpointManager = globalEndpointManager;
	        this.request = request;
	        this.retryAfterInMilliseconds = SessionReadRetryPolicy.retryAfterInMilliseconds;
	    },
	    {
	        /**
	         * Determines whether the request should be retried or not.
	         * @param {object} err - Error returned by the request.
	         * @param {function} callback - The callback function which takes bool argument which specifies whether the request will be retried or not.
	        */
	        shouldRetry: function (err, callback) {
	            if (err) {
	                var that = this;
	                if (this.currentRetryAttemptCount <= this._maxRetryAttemptCount
	                    && (this.request.operationType == Constants.OperationTypes.Read ||
	                        this.request.operationType == Constants.OperationTypes.Query)) {
	                    that.globalEndpointManager.getReadEndpoint(function (readEndpoint) {
	                        that.globalEndpointManager.getWriteEndpoint(function (writeEndpoint) {
	                            if (readEndpoint !== writeEndpoint && that.request.endpointOverride == null) {
	                                that.currentRetryAttemptCount++;
	                                console.log("Read with session token not available in read region. Trying read from write region.");
	                                that.request.endpointOverride = writeEndpoint;
	                                var newUrl = url.parse(writeEndpoint);
	                                return callback(true, newUrl);
	                            } else {
	                                console.log("Clear the the token for named base request");
	                                that.request.client.clearSessionToken(that.request.path);
	                                return callback(false);
	                            }
	                        });
	                    });
	                    return;
	                }
	            }
	            return callback(false);
	        }
	    },
	    {
	        maxRetryAttemptCount: 1,
	        retryAfterInMilliseconds: 0,
	        NOT_FOUND_STATUS_CODE: 404,
	        READ_SESSION_NOT_AVAILABLE_SUB_STATUS_CODE: 1002
	    }
	);
	//SCRIPT END

	if (true) {
	    module.exports = SessionReadRetryPolicy;
	}

/***/ }),
/* 131 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , Constants = __webpack_require__(69)
	    , url = __webpack_require__(94);

	//SCRIPT START
	/**
	     * This internal class implements the logic for endpoint management for geo-replicated
	       database accounts.
	     * @property {object} client                       - The document client instance.
	     * @property {string} defaultEndpoint              - The endpoint used to create the client instance.
	     * @property {bool} enableEndpointDiscovery        - Flag to enable/disable automatic redirecting of requests based on read/write operations.
	     * @property {Array} preferredLocations            - List of azure regions to be used as preferred locations for read requests.
	     * @property {bool} isEndpointCacheInitialized     - Flag to determine whether the endpoint cache is initialized or not.
	*/
	var GlobalEndpointManager = Base.defineClass(
	    /**
	     * @constructor GlobalEndpointManager
	     * @param {object} client                          - The document client instance.
	    */
	    function (client) {
	        this.client = client;
	        this.defaultEndpoint = client.urlConnection;
	        this._readEndpoint = client.urlConnection;
	        this._writeEndpoint = client.urlConnection;
	        this.enableEndpointDiscovery = client.connectionPolicy.EnableEndpointDiscovery;
	        this.preferredLocations = client.connectionPolicy.PreferredLocations;
	        this.isEndpointCacheInitialized = false;
	    }, 
	    {
	        /** Gets the current read endpoint from the endpoint cache.
	         * @memberof GlobalEndpointManager
	         * @instance
	         * @param {function} callback        - The callback function which takes readEndpoint(string) as an argument.
	        */
	        getReadEndpoint: function (callback) {
	            if (!this.isEndpointCacheInitialized) {
	                this.refreshEndpointList(function (writeEndpoint, readEndpoint) {
	                    callback(readEndpoint);
	                });
	            } else {
	                callback(this._readEndpoint);
	            }
	        },
	        
	        /** Sets the current read endpoint.
	         * @memberof GlobalEndpointManager
	         * @instance
	         * @param {string} readEndpoint        - The endpoint to be set as readEndpoint.
	        */
	        setReadEndpoint: function (readEndpoint) {
	            this._readEndpoint = readEndpoint;
	        },
	        
	        /** Gets the current write endpoint from the endpoint cache.
	         * @memberof GlobalEndpointManager
	         * @instance
	         * @param {function} callback        - The callback function which takes writeEndpoint(string) as an argument.
	        */
	        getWriteEndpoint: function (callback) {
	            if (!this.isEndpointCacheInitialized) {
	                this.refreshEndpointList(function (writeEndpoint, readEndpoint) {
	                    callback(writeEndpoint);
	                });
	            } else {
	                callback(this._writeEndpoint);
	            }
	        },
	        
	        /** Sets the current write endpoint.
	         * @memberof GlobalEndpointManager
	         * @instance
	         * @param {string} writeEndpoint        - The endpoint to be set as writeEndpoint.
	        */
	        setWriteEndpoint: function (writeEndpoint) {
	            this._writeEndpoint = writeEndpoint;
	        },
	        
	        /** Refreshes the endpoint list by retrieving the writable and readable locations
	            from the geo-replicated database account and then updating the locations cache.
	            We skip the refreshing if EnableEndpointDiscovery is set to False
	         * @memberof GlobalEndpointManager
	         * @instance
	         * @param {function} callback        - The callback function which takes writeEndpoint(string) and readEndpoint(string) as arguments.
	        */
	        refreshEndpointList: function (callback) {
	            var writableLocations = [];
	            var readableLocations = [];
	            var databaseAccount;
	            
	            var that = this;
	            if (this.enableEndpointDiscovery) {
	                this._getDatabaseAccount(function (databaseAccount) {
	                    if (databaseAccount) {
	                        writableLocations = databaseAccount.WritableLocations;
	                        readableLocations = databaseAccount.ReadableLocations;
	                    }
	                    
	                    // Read and Write endpoints will be initialized to default endpoint if we were not able to get the database account info
	                    that._updateLocationsCache(writableLocations, readableLocations, function (endpoints) {
	                        that._writeEndpoint = endpoints[0];
	                        that._readEndpoint = endpoints[1];
	                        that.isEndpointCacheInitialized = true;
	                        callback(that._writeEndpoint, that._readEndpoint);
	                    });
	                });
	            } else {
	                callback(that._writeEndpoint, that._readEndpoint);
	            }
	        },
	        
	        /** Gets the database account first by using the default endpoint, and if that doesn't returns
	           use the endpoints for the preferred locations in the order they are specified to get 
	           the database account.
	         * @memberof GlobalEndpointManager
	         * @instance
	         * @param {function} callback        - The callback function which takes databaseAccount(object) as an argument.
	        */
	        _getDatabaseAccount: function (callback) {
	            var that = this;
	            var options = { urlConnection: this.defaultEndpoint };
	            this.client.getDatabaseAccount(options, function (err, databaseAccount) {
	                // If for any reason(non - globaldb related), we are not able to get the database account from the above call to getDatabaseAccount,
	                // we would try to get this information from any of the preferred locations that the user might have specified(by creating a locational endpoint)
	                // and keeping eating the exception until we get the database account and return None at the end, if we are not able to get that info from any endpoints

	                if (err) {
	                    var func = function (defaultEndpoint, preferredLocations, index) {
	                        if (index < preferredLocations.length) {
	                            var locationalEndpoint = that._getLocationalEndpoint(defaultEndpoint, preferredLocations[index]);
	                            var options = { urlConnection: locationalEndpoint };
	                            that.client.getDatabaseAccount(options, function (err, databaseAccount) {
	                                if (err) {
	                                    func(defaultEndpoint, preferredLocations, index + 1);
	                                } else {
	                                    return callback(databaseAccount);
	                                }
	                            });
	                        } else {
	                            return callback(null);
	                        }
	                    }
	                    func(that.defaultEndpoint, that.preferredLocations, 0);

	                } else {
	                    return callback(databaseAccount);
	                }
	            });
	        },

	        /** Gets the locational endpoint using the location name passed to it using the default endpoint.
	         * @memberof GlobalEndpointManager
	         * @instance
	         * @param {string} defaultEndpoint - The default endpoint to use for teh endpoint.
	         * @param {string} locationName    - The location name for the azure region like "East US".
	        */
	        _getLocationalEndpoint: function (defaultEndpoint, locationName) {
	            // For defaultEndpoint like 'https://contoso.documents.azure.com:443/' parse it to generate URL format
	            // This defaultEndpoint should be global endpoint(and cannot be a locational endpoint) and we agreed to document that
	            var endpointUrl = url.parse(defaultEndpoint, true, true);
	            
	            // hostname attribute in endpointUrl will return 'contoso.documents.azure.com'
	            if (endpointUrl.hostname) {
	                var hostnameParts = (endpointUrl.hostname).toString().toLowerCase().split(".");
	                if (hostnameParts) {
	                    // globalDatabaseAccountName will return 'contoso'
	                    var globalDatabaseAccountName = hostnameParts[0];
	                    
	                    // Prepare the locationalDatabaseAccountName as contoso-EastUS for location_name 'East US'
	                    var locationalDatabaseAccountName = globalDatabaseAccountName + "-" + locationName.replace(" ", "");
	                    
	                    // Replace 'contoso' with 'contoso-EastUS' and return locationalEndpoint as https://contoso-EastUS.documents.azure.com:443/
	                    var locationalEndpoint = defaultEndpoint.toLowerCase().replace(globalDatabaseAccountName, locationalDatabaseAccountName);
	                    return locationalEndpoint;
	                }
	            }
	            
	            return null;
	        },
	        
	        /** Updates the read and write endpoints from the passed-in readable and writable locations.
	         * @memberof GlobalEndpointManager
	         * @instance
	         * @param {Array} writableLocations     - The list of writable locations for the geo-enabled database account.
	         * @param {Array} readableLocations     - The list of readable locations for the geo-enabled database account.
	         * @param {function} callback           - The function to be called as callback after executing this method.
	        */
	        _updateLocationsCache: function (writableLocations, readableLocations, callback) {
	            var writeEndpoint;
	            var readEndpoint;
	            // Use the default endpoint as Read and Write endpoints if EnableEndpointDiscovery
	            // is set to False.
	            if (!this.enableEndpointDiscovery) {
	                writeEndpoint = this.defaultEndpoint;
	                readEndpoint = this.defaultEndpoint;
	                return callback([writeEndpoint, readEndpoint]);
	            }
	            
	            // Use the default endpoint as Write endpoint if there are no writable locations, or
	            // first writable location as Write endpoint if there are writable locations
	            if (writableLocations.length === 0) {
	                writeEndpoint = this.defaultEndpoint;
	            } else {
	                writeEndpoint = writableLocations[0][Constants.DatabaseAccountEndpoint];
	            }
	            
	            // Use the Write endpoint as Read endpoint if there are no readable locations
	            if (readableLocations.length === 0) {
	                readEndpoint = writeEndpoint;
	                return callback([writeEndpoint, readEndpoint]);
	            } else {
	                // Use the writable location as Read endpoint if there are no preferred locations or
	                // none of the preferred locations are in read or write locations
	                readEndpoint = writeEndpoint;

	                if (!this.preferredLocations) {
	                    return callback([writeEndpoint, readEndpoint]);
	                }

	                for (var i= 0; i < this.preferredLocations.length; i++) {
	                    var preferredLocation = this.preferredLocations[i];
	                    // Use the first readable location as Read endpoint from the preferred locations
	                    for (var j = 0; j < readableLocations.length; j++) {
	                        var readLocation = readableLocations[j];
	                        if (readLocation[Constants.Name] === preferredLocation) {
	                            readEndpoint = readLocation[Constants.DatabaseAccountEndpoint];
	                            return callback([writeEndpoint, readEndpoint]);
	                        }
	                    }
	                    // Else, use the first writable location as Read endpoint from the preferred locations
	                    for (var k = 0; k < writableLocations.length; k++) {
	                        var writeLocation = writableLocations[k];
	                        if (writeLocation[Constants.Name] === preferredLocation) {
	                            readEndpoint = writeLocation[Constants.DatabaseAccountEndpoint];
	                            return callback([writeEndpoint, readEndpoint]);
	                        }
	                    }
	                }

	                return callback([writeEndpoint, readEndpoint]);
	            }
	        }
	    });
	//SCRIPT END

	    if (true) {
	        module.exports = GlobalEndpointManager;
	    }

/***/ }),
/* 132 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict"

	var Base = __webpack_require__(4),
	    Constants = __webpack_require__(69);

	var Regexes = Constants.RegularExpressions,
	    ResourceTypes = Constants.ResourceTypes;


	//SCRIPT START
	var Helper = Base.defineClass(

	    /**************************CONSTRUCTORS**************************/
	    undefined,

	    /************************INSTANCE MEMBERS************************/
	    undefined,

	    /*************************STATIC METHODS*************************/
	    {
	        isStringNullOrEmpty: function (inputString) {
	            //checks whether string is null, undefined, empty or only contains space
	            return !inputString || /^\s*$/.test(inputString);
	        },

	        trimSlashFromLeftAndRight: function (inputString) {
	            if (typeof inputString != 'string') {
	                throw "invalid input: input is not string";
	            }

	            return inputString.replace(Regexes.TrimLeftSlashes, "").replace(Regexes.TrimRightSlashes, "");
	        },

	        validateResourceId: function (resourceId) {
	            // if resourceId is not a string or is empty throw an error
	            if (typeof resourceId !== 'string' || this.isStringNullOrEmpty(resourceId)) {
	                throw "Resource Id must be a string and cannot be undefined, null or empty";
	            }

	            // if resourceId starts or ends with space throw an error
	            if (resourceId[resourceId.length - 1] == " ") {
	                throw "Resource Id cannot end with space";
	            }

	            // if resource id contains illegal characters throw an error
	            if (Regexes.IllegalResourceIdCharacters.test(resourceId)) {
	                throw "Illegal characters ['/', '\\', '?', '#'] cannot be used in resourceId";
	            }

	            return true;

	        },

	        getResourceIdFromPath: function(resourcePath) {
	            if (!resourcePath || typeof resourcePath !== "string") {
	                return null;
	            }

	            var trimmedPath = this.trimSlashFromLeftAndRight(resourcePath);
	            var pathSegments = trimmedPath.split('/');

	            //number of segments of a path must always be even
	            if (pathSegments.length % 2 !== 0) {
	                return null;
	            }

	            return pathSegments[pathSegments.length - 1];
	        }
	    }

	);
	//SCRIPT END

	if (true) {
	    exports.Helper = Helper;
	}

/***/ }),
/* 133 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , ResourceId = __webpack_require__(134)
	    , Constants = __webpack_require__(69)
	    , BigInt = __webpack_require__(135);

	var SessionContainer = Base.defineClass(

	    function (hostname, collectionNameToCollectionResourceId, collectionResourceIdToSessionTokens) {
	        this.hostname = hostname;

	        if (collectionNameToCollectionResourceId != undefined && collectionResourceIdToSessionTokens != undefined) {
	            this.collectionNameToCollectionResourceId = collectionNameToCollectionResourceId;
	            this.collectionResourceIdToSessionTokens = collectionResourceIdToSessionTokens;
	        } else {
	            this.collectionNameToCollectionResourceId = {};
	            this.collectionResourceIdToSessionTokens = {};
	        }
	    },
	    {
	        getHostName: function () {
	            return this.hostname;
	        },

	        getPartitionKeyRangeIdToTokenMap: function (request) {
	            return this.getPartitionKeyRangeIdToTokenMapPrivate(request['isNameBased'], request['resourceId'], request['resourceAddress']);
	        },

	        getPartitionKeyRangeIdToTokenMapPrivate: function (isNameBased, rId, resourceAddress) {
	            var rangeIdToTokenMap = null;
	            if (!isNameBased) {
	                if (rId) {
	                    var resourceIdObject = new ResourceId();
	                    var resourceId = resourceIdObject.parse(rId);
	                    if (resourceId.documentCollection != '0') {
	                        rangeIdToTokenMap = this.collectionResourceIdToSessionTokens[resourceId.getUniqueDocumentCollectionId()];
	                    }
	                }
	            } else {
	                resourceAddress = Base._trimSlashes(resourceAddress)
	                var collectionName = Base.getCollectionLink(resourceAddress);
	                if (collectionName && (collectionName in this.collectionNameToCollectionResourceId))
	                    rangeIdToTokenMap = this.collectionResourceIdToSessionTokens[this.collectionNameToCollectionResourceId[collectionName]];
	            }

	            return rangeIdToTokenMap;
	        },

	        resolveGlobalSessionToken: function (request) {
	            if (!request)
	                throw new Error("request cannot be null");

	            return this.resolveGlobalSessionTokenPrivate(request['isNameBased'], request['resourceId'], request['resourceAddress']);
	        },

	        resolveGlobalSessionTokenPrivate: function (isNameBased, rId, resourceAddress) {
	            var rangeIdToTokenMap = this.getPartitionKeyRangeIdToTokenMapPrivate(isNameBased, rId, resourceAddress);
	            if (rangeIdToTokenMap != null)
	                return this.getCombinedSessionToken(rangeIdToTokenMap);

	            return "";
	        },

	        clearToken: function (request) {
	            var collectionResourceId = undefined;
	            if (!request['isNameBased']) {
	                if (request['resourceId']) {
	                    var resourceIdObject = new ResourceId();
	                    var resourceId = resourceIdObject.parse(request['resourceId']);
	                    if (resourceId.documentCollection != 0) {
	                        collectionResourceId = resourceId.getUniqueDocumentCollectionId();
	                    }
	                }
	            } else {
	                var resourceAddress = Base._trimSlashes(request['resourceAddress']);
	                var collectionName = Base.getCollectionLink(resourceAddress);
	                if (collectionName) {
	                    collectionResourceId = this.collectionNameToCollectionResourceId[collectionName];
	                    delete this.collectionNameToCollectionResourceId[collectionName];
	                }
	            }
	            if (collectionResourceId != undefined)
	                delete this.collectionResourceIdToSessionTokens[collectionResourceId];
	        },

	        setSessionToken: function (request, reqHeaders, resHeaders) {
	            if (resHeaders && !this.isReadingFromMaster(request['resourceType'], request['opearationType'])) {
	                var sessionToken = resHeaders[Constants.HttpHeaders.SessionToken];
	                if (sessionToken) {
	                    var ownerFullName = resHeaders[Constants.HttpHeaders.OwnerFullName];
	                    if (!ownerFullName)
	                        ownerFullName = Base._trimSlashes(request['resourceAddress']);

	                    var collectionName = Base.getCollectionLink(ownerFullName);

	                    var ownerId = undefined;
	                    if (!request['isNameBased']) {
	                        ownerId = request['resourceId'];
	                    } else {
	                        ownerId = resHeaders[Constants.HttpHeaders.OwnerId];
	                        if (!ownerId)
	                            ownerId = request['resourceId'];
	                    }

	                    if (ownerId) {
	                        var resourceIdObject = new ResourceId();
	                        var resourceId = resourceIdObject.parse(ownerId);

	                        if (resourceId.documentCollection != 0 && collectionName) {
	                            var uniqueDocumentCollectionId = resourceId.getUniqueDocumentCollectionId();
	                            this.setSesisonTokenPrivate(uniqueDocumentCollectionId, collectionName, sessionToken);
	                        }
	                    }
	                }
	            }
	        },

	        setSesisonTokenPrivate: function (collectionRid, collectionName, sessionToken) {
	            if (!(collectionRid in this.collectionResourceIdToSessionTokens))
	                this.collectionResourceIdToSessionTokens[collectionRid] = {};
	            this.compareAndSetToken(sessionToken, this.collectionResourceIdToSessionTokens[collectionRid]);
	            if (!(collectionName in this.collectionNameToCollectionResourceId))
	                this.collectionNameToCollectionResourceId[collectionName] = collectionRid;
	        },

	        getCombinedSessionToken: function (tokens) {
	            var result = "";
	            if (tokens) {
	                for (var index in tokens) {
	                    result = result + index + ':' + tokens[index] + ",";
	                }
	            }
	            return result.slice(0, -1);
	        },

	        compareAndSetToken: function (newToken, oldTokens) {
	            if (newToken) {
	                var newTokenParts = newToken.split(":");
	                if (newTokenParts.length == 2) {
	                    var range = newTokenParts[0];
	                    var newLSN = BigInt(newTokenParts[1]);
	                    var success = false;

	                    var oldLSN = BigInt(oldTokens[range]);
	                    if (!oldLSN || oldLSN.lesser(newLSN))
	                        oldTokens[range] = newLSN.toString();
	                }
	            }
	        },

	        isReadingFromMaster: function (resourceType, operationType) {
	            if (resourceType == "offers" ||
	                resourceType == "dbs" ||
	                resourceType == "users" ||
	                resourceType == "permissions" ||
	                resourceType == "topology" ||
	                resourceType == "databaseaccount" ||
	                resourceType == "pkranges" ||
	                (resourceType == "colls"
	                    && (operationType == Constants.OperationTypes.Query))) {
	                return true;
	            }

	            return false;
	        }
	    }
	);

	if (true) {
	    module.exports = SessionContainer;
	}



/***/ }),
/* 134 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , BigInt = __webpack_require__(135)
	    , Int64BE = __webpack_require__(136).Int64BE;

	//SCRIPT START
	var ResourceId = Base.defineClass(

	    function () {
	        this.offer = '0';
	        this.database = '0';
	        this.documentCollection = '0';
	        this.storedProcedure = '0';
	        this.trigger = '0';
	        this.userDefinedFunction = '0';
	        this.document = '0';
	        this.partitionKeyRange = '0';
	        this.user = '0';
	        this.conflict = '0';
	        this.permission = '0';
	        this.attachment = '0';
	        this.length = 20,
	            this.offer_id_length = 3,
	            this.DocumentByte = 0,
	            this.StoredProcedureByte = 8,
	            this.TriggerByte = 7,
	            this.UserDefinedFunctionByte = 6,
	            this.ConflictByte = 4,
	            this.PartitionKeyRangeByte = 5

	    },
	    {
	        parse: function (id) {
	            var pair = this.tryParse(id);

	            if (!pair[0]) {
	                throw (new Error("invalid resource id " + id));
	            }
	            return pair[1];
	        },

	        newDatabaseId: function (dbId) {
	            var resourceId = new ResourceId();
	            resourceId.database = dbId;
	            return ResourceId;
	        },

	        newDocumentCollectionId: function (databaseId, collectionId) {
	            var dbId = this.parse(databaseId);

	            var collectionResourceId = new ResourceId();
	            collectionResourceId.database = dbId.database;
	            collectionResourceId.documentCollection = collectionId;

	            return collectionResourceId;
	        },

	        newUserId: function (databaseId, userId) {
	            var dbId = this.parse(databaseId);

	            var userResourceId = new ResourceId();
	            userResourceId.database = dbId.database;
	            userResourceId.user = userId;

	            return userResourceId;
	        },

	        newPermissionId: function (userId, permissionId) {
	            var usrId = this.parse(userId);

	            var permissionResourceId = new ResourceId();
	            permissionResourceId.database = usrId.database;
	            permissionResourceId.user = usrId.user;
	            permissionResourceId.permission = permissionId;
	            return permissionResourceId;
	        },

	        newAttachmentId: function (documentId, attachmentId) {
	            var docId = this.parse(documentId);

	            var attachmentResourceId = new ResourceId();
	            attachmentResourceId.database = docId.database;
	            attachmentResourceId.documentCollection = docId.documentCollection;
	            attachmentResourceId.document = docId.document;
	            attachmentResourceId.attachment = attachmentid;

	            return attachmentResourceId;
	        },

	        tryParse: function (id) {
	            var rid = undefined;
	            if (!id)
	                return [false, undefined];

	            var pair = this.verify(id);

	            if (!pair[0])
	                return [false, undefined];

	            var buffer = pair[1];

	            var intArray = new Int8Array(buffer);

	            if (buffer.length % 4 != 0 && buffer.length != this.offer_id_length)
	                return [false, undefined];

	            var rid = new ResourceId();

	            //if length < 4 bytes, the resource is an offer 
	            if (buffer.length == this.offer_id_length) {
	                rid.offer = 0;

	                for (var index = 0; index < this.offer_id_length; index++) {
	                    rid.offer = rid.offer | (intArray[index] << (index * 8));
	                }

	                rid.offer = rid.offer.toString();
	                return [true, rid];
	            }

	            //first 4 bytes represent the database
	            if (buffer.length >= 4)
	                rid.database = buffer.readIntBE(0, 4).toString();

	            if (buffer.length >= 8) {
	                var isCollection = (intArray[4] & (128)) > 0;

	                if (isCollection) {
	                    //5th - 8th bytes represents the collection

	                    rid.documentCollection = buffer.readIntBE(4, 4).toString();
	                    var newBuff = new Buffer(4);

	                    if (buffer.length >= 16) {

	                        //9th - 15th bytes represent one of document, trigger, sproc, udf, conflict, pkrange
	                        var subCollectionResource = this.bigNumberReadIntBE(buffer, 8, 8).toString();

	                        if ((intArray[15] >> 4) == this.DocumentByte) {
	                            rid.document = subCollectionResource;

	                            //16th - 20th bytes represent the attachment
	                            if (buffer.length == 20)
	                                rid.attachment = buffer.readIntBE(16, 4).toString();
	                        } else if (Math.abs(intArray[15] >> 4) == this.StoredProcedureByte)
	                            rid.storedProcedure = subCollectionResource;
	                        else if ((intArray[15] >> 4) == this.TriggerByte)
	                            rid.trigger = subCollectionResource;
	                        else if ((intArray[15] >> 4) == this.UserDefinedFunctionByte)
	                            rid.userDefinedFunction = subCollectionResource;
	                        else if ((intArray[15] >> 4) == this.ConflictByte)
	                            rid.conflict = subCollectionResource;
	                        else if ((intArray[15] >> 4) == this.PartitionKeyRangeByte)
	                            rid.partitionKeyRange = subCollectionResource;
	                        else
	                            return [false, rid];

	                    } else if (buffer.length != 8) {
	                        return [false, rid];
	                    }
	                } else {
	                    //5th - 8th bytes represents the user

	                    rid.user = buffer.readIntBE(4, 4).toString();

	                    //9th - 15th bytes represent the permission
	                    if (buffer.length == 16)
	                        rid.permission = this.bigNumberReadIntBE(buffer, 8, 8).toString();
	                    else if (buffer.length != 8)
	                        return [false, rid];
	                }
	            }

	            return [true, rid];
	        },

	        verify: function (id) {
	            if (!id) {
	                throw (new Error("invalid resource id " + id));
	            }

	            var buffer = this.fromBase64String(id);
	            if (!buffer || buffer.length > this.length) {
	                buffer = undefined;
	                return [false, buffer];
	            }

	            return [true, buffer];
	        },

	        verifyBool: function (id) {
	            return this.verify(id)[0];
	        },

	        fromBase64String: function (s) {
	            return Buffer(s.replace('-', '/'), 'base64');
	        },

	        toBase64String: function (buffer) {
	            return buffer.toString('base64');
	        },

	        isDatabaseId: function () {
	            return this.database != 0 && (this.documentCollection == 0 && this.user == 0)
	        },

	        getDatabaseId: function () {
	            var rid = new ResourceId();
	            rid.database = this.database;
	            return rid;
	        },

	        getDocumentCollectionId: function () {
	            var rid = new ResourceId();
	            rid.database = this.database;
	            rid.documentCollection = this.documentCollection;
	            return rid;
	        },

	        getUniqueDocumentCollectionId: function () {
	            var db = new BigInt(this.database);
	            var coll = new BigInt(this.documentCollection);
	            return db.shiftLeft(32).or(coll).toString();
	        },

	        getStoredProcedureId: function () {
	            var rid = new ResourceId();
	            rid.database = this.database;
	            rid.documentCollection = this.documentCollection;
	            rid.storedProcedure = this.storedProcedure;
	            return rid;
	        },

	        getTriggerId: function () {
	            var rid = new ResourceId();
	            rid.database = this.database;
	            rid.documentCollection = this.documentCollection;
	            rid.trigger = this.trigger;
	            return rid;
	        },

	        getUserDefinedFunctionId: function () {
	            var rid = new ResourceId();
	            rid.database = this.database;
	            rid.documentCollection = this.documentCollection;
	            rid.userDefinedFunction = this.userDefinedFunction;
	            return rid;
	        },

	        getConflictId: function () {
	            var rid = new ResourceId();
	            rid.database = this.database;
	            rid.documentCollection = this.documentCollection;
	            rid.conflict = this.conflict;
	            return rid;
	        },

	        getDocumentId: function () {
	            var rid = new ResourceId();
	            rid.database = this.database;
	            rid.documentCollection = this.documentCollection;
	            rid.document = this.document;
	            return rid;
	        },

	        getPartitonKeyRangeId: function () {
	            var rid = new ResourceId();
	            rid.database = this.database;
	            rid.documentCollection = this.documentCollection;
	            rid.partitionKeyRange = this.partitionKeyRange;
	            return rid;
	        },

	        getUserId: function () {
	            var rid = new ResourceId();
	            rid.database = this.database;
	            rid.user = this.user;
	            return rid;
	        },

	        getPermissionId: function () {
	            var rid = new ResourceId();
	            rid.database = this.database;
	            rid.user = this.user;
	            rid.permission = this.permission;
	            return rid;
	        },

	        getAttachmentId: function () {
	            var rid = new ResourceId();
	            rid.database = this.database;
	            rid.documentCollection = this.documentCollection;
	            rid.document = this.document;
	            rid.attachment = this.attachment;
	            return rid;
	        },

	        getOfferId: function () {
	            var rid = new ResourceId();
	            rid.offer = this.offer;
	            return rid;
	        },

	        getValue: function () {
	            var len = 0;
	            if (this.offer != '0')
	                len = len + this.offer_id_length;
	            else if (this.database != '0')
	                len = len + 4;
	            if (this.documentCollection != '0' || this.user != '0')
	                len = len + 4;
	            if (this.document != '0' || this.permission != '0'
	                || this.storedProcedure != '0' || this.trigger != '0'
	                || this.userDefinedFunction != 0 || this.conflict != '0'
	                || this.partitionKeyRange != '0')
	                len = len + 8;
	            if (this.attachment != '0')
	                len = len + 4;

	            var buffer = new Buffer(len);
	            buffer.fill(0);

	            if (this.offer != '0')
	                buffer.writeIntLE(Number(this.offer), 0, this.offer_id_length);
	            else if (this.database != '0')
	                buffer.writeIntBE(Number(this.database), 0, 4);

	            if (this.documentCollection != '0')
	                buffer.writeIntBE(Number(this.documentCollection), 4, 4);
	            else if (this.user != '0')
	                buffer.writeIntBE(Number(this.user), 4, 4);

	            if (this.storedProcedure != '0') {
	                var big = new Int64BE(this.storedProcedure);
	                big.toBuffer().copy(buffer, 8, 0, 8);
	            }
	            else if (this.trigger != '0') {
	                var big = new Int64BE(this.trigger);
	                big.toBuffer().copy(buffer, 8, 0, 8);
	            }
	            else if (this.userDefinedFunction != '0') {
	                var big = new Int64BE(this.userDefinedFunction);
	                big.toBuffer().copy(buffer, 8, 0, 8);
	            }
	            else if (this.conflict != '0') {
	                var big = new Int64BE(this.conflict);
	                big.toBuffer().copy(buffer, 8, 0, 8);
	            }
	            else if (this.document != '0') {
	                var big = new Int64BE(this.document);
	                big.toBuffer().copy(buffer, 8, 0, 8);
	            }
	            else if (this.permission != '0') {
	                var big = new Int64BE(this.permission);
	                big.toBuffer().copy(buffer, 8, 0, 8);
	            }
	            else if (this.partitionKeyRange != '0') {
	                var big = new Int64BE(this.partitionKeyRange);
	                big.toBuffer().copy(buffer, 8, 0, 8);
	            }

	            if (this.attachment != '0')
	                buffer.writeIntBE(Number(this.attachment), 16, 4);

	            return buffer;

	        },

	        toString: function () {
	            return this.toBase64String(this.getValue());
	        },

	        bigNumberReadIntBE: function (buffer, offset, byteLength) {
	            offset = offset >>> 0
	            byteLength = byteLength >>> 0

	            var i = byteLength
	            var mul = new BigInt("1");
	            var val = new BigInt(buffer[offset + --i]);
	            while (i > 0 && (mul = mul.times(0x100))) {
	                var temp = new BigInt(buffer[offset + --i]);
	                val = val.plus(temp.times(mul));
	            }
	            mul = mul.times(0x80);

	            if (val.greater(mul)) {
	                var subtrahend = new BigInt(2);
	                val = val.minus(subtrahend.pow(8 * byteLength));
	            }

	            return val
	        }
	    }, null
	);
	//SCRIPT END

	if (true) {
	    module.exports = ResourceId;
	}

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 135 */
/***/ (function(module, exports, __webpack_require__) {

	var __WEBPACK_AMD_DEFINE_ARRAY__, __WEBPACK_AMD_DEFINE_RESULT__;/* WEBPACK VAR INJECTION */(function(module) {var bigInt = (function (undefined) {
	    "use strict";

	    var BASE = 1e7,
	        LOG_BASE = 7,
	        MAX_INT = 9007199254740992,
	        MAX_INT_ARR = smallToArray(MAX_INT),
	        LOG_MAX_INT = Math.log(MAX_INT);

	    function Integer(v, radix) {
	        if (typeof v === "undefined") return Integer[0];
	        if (typeof radix !== "undefined") return +radix === 10 ? parseValue(v) : parseBase(v, radix);
	        return parseValue(v);
	    }

	    function BigInteger(value, sign) {
	        this.value = value;
	        this.sign = sign;
	        this.isSmall = false;
	    }
	    BigInteger.prototype = Object.create(Integer.prototype);

	    function SmallInteger(value) {
	        this.value = value;
	        this.sign = value < 0;
	        this.isSmall = true;
	    }
	    SmallInteger.prototype = Object.create(Integer.prototype);

	    function isPrecise(n) {
	        return -MAX_INT < n && n < MAX_INT;
	    }

	    function smallToArray(n) { // For performance reasons doesn't reference BASE, need to change this function if BASE changes
	        if (n < 1e7)
	            return [n];
	        if (n < 1e14)
	            return [n % 1e7, Math.floor(n / 1e7)];
	        return [n % 1e7, Math.floor(n / 1e7) % 1e7, Math.floor(n / 1e14)];
	    }

	    function arrayToSmall(arr) { // If BASE changes this function may need to change
	        trim(arr);
	        var length = arr.length;
	        if (length < 4 && compareAbs(arr, MAX_INT_ARR) < 0) {
	            switch (length) {
	                case 0: return 0;
	                case 1: return arr[0];
	                case 2: return arr[0] + arr[1] * BASE;
	                default: return arr[0] + (arr[1] + arr[2] * BASE) * BASE;
	            }
	        }
	        return arr;
	    }

	    function trim(v) {
	        var i = v.length;
	        while (v[--i] === 0);
	        v.length = i + 1;
	    }

	    function createArray(length) { // function shamelessly stolen from Yaffle's library https://github.com/Yaffle/BigInteger
	        var x = new Array(length);
	        var i = -1;
	        while (++i < length) {
	            x[i] = 0;
	        }
	        return x;
	    }

	    function truncate(n) {
	        if (n > 0) return Math.floor(n);
	        return Math.ceil(n);
	    }

	    function add(a, b) { // assumes a and b are arrays with a.length >= b.length
	        var l_a = a.length,
	            l_b = b.length,
	            r = new Array(l_a),
	            carry = 0,
	            base = BASE,
	            sum, i;
	        for (i = 0; i < l_b; i++) {
	            sum = a[i] + b[i] + carry;
	            carry = sum >= base ? 1 : 0;
	            r[i] = sum - carry * base;
	        }
	        while (i < l_a) {
	            sum = a[i] + carry;
	            carry = sum === base ? 1 : 0;
	            r[i++] = sum - carry * base;
	        }
	        if (carry > 0) r.push(carry);
	        return r;
	    }

	    function addAny(a, b) {
	        if (a.length >= b.length) return add(a, b);
	        return add(b, a);
	    }

	    function addSmall(a, carry) { // assumes a is array, carry is number with 0 <= carry < MAX_INT
	        var l = a.length,
	            r = new Array(l),
	            base = BASE,
	            sum, i;
	        for (i = 0; i < l; i++) {
	            sum = a[i] - base + carry;
	            carry = Math.floor(sum / base);
	            r[i] = sum - carry * base;
	            carry += 1;
	        }
	        while (carry > 0) {
	            r[i++] = carry % base;
	            carry = Math.floor(carry / base);
	        }
	        return r;
	    }

	    BigInteger.prototype.add = function (v) {
	        var n = parseValue(v);
	        if (this.sign !== n.sign) {
	            return this.subtract(n.negate());
	        }
	        var a = this.value, b = n.value;
	        if (n.isSmall) {
	            return new BigInteger(addSmall(a, Math.abs(b)), this.sign);
	        }
	        return new BigInteger(addAny(a, b), this.sign);
	    };
	    BigInteger.prototype.plus = BigInteger.prototype.add;

	    SmallInteger.prototype.add = function (v) {
	        var n = parseValue(v);
	        var a = this.value;
	        if (a < 0 !== n.sign) {
	            return this.subtract(n.negate());
	        }
	        var b = n.value;
	        if (n.isSmall) {
	            if (isPrecise(a + b)) return new SmallInteger(a + b);
	            b = smallToArray(Math.abs(b));
	        }
	        return new BigInteger(addSmall(b, Math.abs(a)), a < 0);
	    };
	    SmallInteger.prototype.plus = SmallInteger.prototype.add;

	    function subtract(a, b) { // assumes a and b are arrays with a >= b
	        var a_l = a.length,
	            b_l = b.length,
	            r = new Array(a_l),
	            borrow = 0,
	            base = BASE,
	            i, difference;
	        for (i = 0; i < b_l; i++) {
	            difference = a[i] - borrow - b[i];
	            if (difference < 0) {
	                difference += base;
	                borrow = 1;
	            } else borrow = 0;
	            r[i] = difference;
	        }
	        for (i = b_l; i < a_l; i++) {
	            difference = a[i] - borrow;
	            if (difference < 0) difference += base;
	            else {
	                r[i++] = difference;
	                break;
	            }
	            r[i] = difference;
	        }
	        for (; i < a_l; i++) {
	            r[i] = a[i];
	        }
	        trim(r);
	        return r;
	    }

	    function subtractAny(a, b, sign) {
	        var value;
	        if (compareAbs(a, b) >= 0) {
	            value = subtract(a,b);
	        } else {
	            value = subtract(b, a);
	            sign = !sign;
	        }
	        value = arrayToSmall(value);
	        if (typeof value === "number") {
	            if (sign) value = -value;
	            return new SmallInteger(value);
	        }
	        return new BigInteger(value, sign);
	    }

	    function subtractSmall(a, b, sign) { // assumes a is array, b is number with 0 <= b < MAX_INT
	        var l = a.length,
	            r = new Array(l),
	            carry = -b,
	            base = BASE,
	            i, difference;
	        for (i = 0; i < l; i++) {
	            difference = a[i] + carry;
	            carry = Math.floor(difference / base);
	            difference %= base;
	            r[i] = difference < 0 ? difference + base : difference;
	        }
	        r = arrayToSmall(r);
	        if (typeof r === "number") {
	            if (sign) r = -r;
	            return new SmallInteger(r);
	        } return new BigInteger(r, sign);
	    }

	    BigInteger.prototype.subtract = function (v) {
	        var n = parseValue(v);
	        if (this.sign !== n.sign) {
	            return this.add(n.negate());
	        }
	        var a = this.value, b = n.value;
	        if (n.isSmall)
	            return subtractSmall(a, Math.abs(b), this.sign);
	        return subtractAny(a, b, this.sign);
	    };
	    BigInteger.prototype.minus = BigInteger.prototype.subtract;

	    SmallInteger.prototype.subtract = function (v) {
	        var n = parseValue(v);
	        var a = this.value;
	        if (a < 0 !== n.sign) {
	            return this.add(n.negate());
	        }
	        var b = n.value;
	        if (n.isSmall) {
	            return new SmallInteger(a - b);
	        }
	        return subtractSmall(b, Math.abs(a), a >= 0);
	    };
	    SmallInteger.prototype.minus = SmallInteger.prototype.subtract;

	    BigInteger.prototype.negate = function () {
	        return new BigInteger(this.value, !this.sign);
	    };
	    SmallInteger.prototype.negate = function () {
	        var sign = this.sign;
	        var small = new SmallInteger(-this.value);
	        small.sign = !sign;
	        return small;
	    };

	    BigInteger.prototype.abs = function () {
	        return new BigInteger(this.value, false);
	    };
	    SmallInteger.prototype.abs = function () {
	        return new SmallInteger(Math.abs(this.value));
	    };

	    function multiplyLong(a, b) {
	        var a_l = a.length,
	            b_l = b.length,
	            l = a_l + b_l,
	            r = createArray(l),
	            base = BASE,
	            product, carry, i, a_i, b_j;
	        for (i = 0; i < a_l; ++i) {
	            a_i = a[i];
	            for (var j = 0; j < b_l; ++j) {
	                b_j = b[j];
	                product = a_i * b_j + r[i + j];
	                carry = Math.floor(product / base);
	                r[i + j] = product - carry * base;
	                r[i + j + 1] += carry;
	            }
	        }
	        trim(r);
	        return r;
	    }

	    function multiplySmall(a, b) { // assumes a is array, b is number with |b| < BASE
	        var l = a.length,
	            r = new Array(l),
	            base = BASE,
	            carry = 0,
	            product, i;
	        for (i = 0; i < l; i++) {
	            product = a[i] * b + carry;
	            carry = Math.floor(product / base);
	            r[i] = product - carry * base;
	        }
	        while (carry > 0) {
	            r[i++] = carry % base;
	            carry = Math.floor(carry / base);
	        }
	        return r;
	    }

	    function shiftLeft(x, n) {
	        var r = [];
	        while (n-- > 0) r.push(0);
	        return r.concat(x);
	    }

	    function multiplyKaratsuba(x, y) {
	        var n = Math.max(x.length, y.length);

	        if (n <= 30) return multiplyLong(x, y);
	        n = Math.ceil(n / 2);

	        var b = x.slice(n),
	            a = x.slice(0, n),
	            d = y.slice(n),
	            c = y.slice(0, n);

	        var ac = multiplyKaratsuba(a, c),
	            bd = multiplyKaratsuba(b, d),
	            abcd = multiplyKaratsuba(addAny(a, b), addAny(c, d));

	        var product = addAny(addAny(ac, shiftLeft(subtract(subtract(abcd, ac), bd), n)), shiftLeft(bd, 2 * n));
	        trim(product);
	        return product;
	    }

	    // The following function is derived from a surface fit of a graph plotting the performance difference
	    // between long multiplication and karatsuba multiplication versus the lengths of the two arrays.
	    function useKaratsuba(l1, l2) {
	        return -0.012 * l1 - 0.012 * l2 + 0.000015 * l1 * l2 > 0;
	    }

	    BigInteger.prototype.multiply = function (v) {
	        var n = parseValue(v),
	            a = this.value, b = n.value,
	            sign = this.sign !== n.sign,
	            abs;
	        if (n.isSmall) {
	            if (b === 0) return Integer[0];
	            if (b === 1) return this;
	            if (b === -1) return this.negate();
	            abs = Math.abs(b);
	            if (abs < BASE) {
	                return new BigInteger(multiplySmall(a, abs), sign);
	            }
	            b = smallToArray(abs);
	        }
	        if (useKaratsuba(a.length, b.length)) // Karatsuba is only faster for certain array sizes
	            return new BigInteger(multiplyKaratsuba(a, b), sign);
	        return new BigInteger(multiplyLong(a, b), sign);
	    };

	    BigInteger.prototype.times = BigInteger.prototype.multiply;

	    function multiplySmallAndArray(a, b, sign) { // a >= 0
	        if (a < BASE) {
	            return new BigInteger(multiplySmall(b, a), sign);
	        }
	        return new BigInteger(multiplyLong(b, smallToArray(a)), sign);
	    }
	    SmallInteger.prototype._multiplyBySmall = function (a) {
	            if (isPrecise(a.value * this.value)) {
	                return new SmallInteger(a.value * this.value);
	            }
	            return multiplySmallAndArray(Math.abs(a.value), smallToArray(Math.abs(this.value)), this.sign !== a.sign);
	    };
	    BigInteger.prototype._multiplyBySmall = function (a) {
	            if (a.value === 0) return Integer[0];
	            if (a.value === 1) return this;
	            if (a.value === -1) return this.negate();
	            return multiplySmallAndArray(Math.abs(a.value), this.value, this.sign !== a.sign);
	    };
	    SmallInteger.prototype.multiply = function (v) {
	        return parseValue(v)._multiplyBySmall(this);
	    };
	    SmallInteger.prototype.times = SmallInteger.prototype.multiply;

	    function square(a) {
	        var l = a.length,
	            r = createArray(l + l),
	            base = BASE,
	            product, carry, i, a_i, a_j;
	        for (i = 0; i < l; i++) {
	            a_i = a[i];
	            for (var j = 0; j < l; j++) {
	                a_j = a[j];
	                product = a_i * a_j + r[i + j];
	                carry = Math.floor(product / base);
	                r[i + j] = product - carry * base;
	                r[i + j + 1] += carry;
	            }
	        }
	        trim(r);
	        return r;
	    }

	    BigInteger.prototype.square = function () {
	        return new BigInteger(square(this.value), false);
	    };

	    SmallInteger.prototype.square = function () {
	        var value = this.value * this.value;
	        if (isPrecise(value)) return new SmallInteger(value);
	        return new BigInteger(square(smallToArray(Math.abs(this.value))), false);
	    };

	    function divMod1(a, b) { // Left over from previous version. Performs faster than divMod2 on smaller input sizes.
	        var a_l = a.length,
	            b_l = b.length,
	            base = BASE,
	            result = createArray(b.length),
	            divisorMostSignificantDigit = b[b_l - 1],
	            // normalization
	            lambda = Math.ceil(base / (2 * divisorMostSignificantDigit)),
	            remainder = multiplySmall(a, lambda),
	            divisor = multiplySmall(b, lambda),
	            quotientDigit, shift, carry, borrow, i, l, q;
	        if (remainder.length <= a_l) remainder.push(0);
	        divisor.push(0);
	        divisorMostSignificantDigit = divisor[b_l - 1];
	        for (shift = a_l - b_l; shift >= 0; shift--) {
	            quotientDigit = base - 1;
	            if (remainder[shift + b_l] !== divisorMostSignificantDigit) {
	              quotientDigit = Math.floor((remainder[shift + b_l] * base + remainder[shift + b_l - 1]) / divisorMostSignificantDigit);
	            }
	            // quotientDigit <= base - 1
	            carry = 0;
	            borrow = 0;
	            l = divisor.length;
	            for (i = 0; i < l; i++) {
	                carry += quotientDigit * divisor[i];
	                q = Math.floor(carry / base);
	                borrow += remainder[shift + i] - (carry - q * base);
	                carry = q;
	                if (borrow < 0) {
	                    remainder[shift + i] = borrow + base;
	                    borrow = -1;
	                } else {
	                    remainder[shift + i] = borrow;
	                    borrow = 0;
	                }
	            }
	            while (borrow !== 0) {
	                quotientDigit -= 1;
	                carry = 0;
	                for (i = 0; i < l; i++) {
	                    carry += remainder[shift + i] - base + divisor[i];
	                    if (carry < 0) {
	                        remainder[shift + i] = carry + base;
	                        carry = 0;
	                    } else {
	                        remainder[shift + i] = carry;
	                        carry = 1;
	                    }
	                }
	                borrow += carry;
	            }
	            result[shift] = quotientDigit;
	        }
	        // denormalization
	        remainder = divModSmall(remainder, lambda)[0];
	        return [arrayToSmall(result), arrayToSmall(remainder)];
	    }

	    function divMod2(a, b) { // Implementation idea shamelessly stolen from Silent Matt's library http://silentmatt.com/biginteger/
	        // Performs faster than divMod1 on larger input sizes.
	        var a_l = a.length,
	            b_l = b.length,
	            result = [],
	            part = [],
	            base = BASE,
	            guess, xlen, highx, highy, check;
	        while (a_l) {
	            part.unshift(a[--a_l]);
	            trim(part);
	            if (compareAbs(part, b) < 0) {
	                result.push(0);
	                continue;
	            }
	            xlen = part.length;
	            highx = part[xlen - 1] * base + part[xlen - 2];
	            highy = b[b_l - 1] * base + b[b_l - 2];
	            if (xlen > b_l) {
	                highx = (highx + 1) * base;
	            }
	            guess = Math.ceil(highx / highy);
	            do {
	                check = multiplySmall(b, guess);
	                if (compareAbs(check, part) <= 0) break;
	                guess--;
	            } while (guess);
	            result.push(guess);
	            part = subtract(part, check);
	        }
	        result.reverse();
	        return [arrayToSmall(result), arrayToSmall(part)];
	    }

	    function divModSmall(value, lambda) {
	        var length = value.length,
	            quotient = createArray(length),
	            base = BASE,
	            i, q, remainder, divisor;
	        remainder = 0;
	        for (i = length - 1; i >= 0; --i) {
	            divisor = remainder * base + value[i];
	            q = truncate(divisor / lambda);
	            remainder = divisor - q * lambda;
	            quotient[i] = q | 0;
	        }
	        return [quotient, remainder | 0];
	    }

	    function divModAny(self, v) {
	        var value, n = parseValue(v);
	        var a = self.value, b = n.value;
	        var quotient;
	        if (b === 0) throw new Error("Cannot divide by zero");
	        if (self.isSmall) {
	            if (n.isSmall) {
	                return [new SmallInteger(truncate(a / b)), new SmallInteger(a % b)];
	            }
	            return [Integer[0], self];
	        }
	        if (n.isSmall) {
	            if (b === 1) return [self, Integer[0]];
	            if (b == -1) return [self.negate(), Integer[0]];
	            var abs = Math.abs(b);
	            if (abs < BASE) {
	                value = divModSmall(a, abs);
	                quotient = arrayToSmall(value[0]);
	                var remainder = value[1];
	                if (self.sign) remainder = -remainder;
	                if (typeof quotient === "number") {
	                    if (self.sign !== n.sign) quotient = -quotient;
	                    return [new SmallInteger(quotient), new SmallInteger(remainder)];
	                }
	                return [new BigInteger(quotient, self.sign !== n.sign), new SmallInteger(remainder)];
	            }
	            b = smallToArray(abs);
	        }
	        var comparison = compareAbs(a, b);
	        if (comparison === -1) return [Integer[0], self];
	        if (comparison === 0) return [Integer[self.sign === n.sign ? 1 : -1], Integer[0]];

	        // divMod1 is faster on smaller input sizes
	        if (a.length + b.length <= 200)
	            value = divMod1(a, b);
	        else value = divMod2(a, b);

	        quotient = value[0];
	        var qSign = self.sign !== n.sign,
	            mod = value[1],
	            mSign = self.sign;
	        if (typeof quotient === "number") {
	            if (qSign) quotient = -quotient;
	            quotient = new SmallInteger(quotient);
	        } else quotient = new BigInteger(quotient, qSign);
	        if (typeof mod === "number") {
	            if (mSign) mod = -mod;
	            mod = new SmallInteger(mod);
	        } else mod = new BigInteger(mod, mSign);
	        return [quotient, mod];
	    }

	    BigInteger.prototype.divmod = function (v) {
	        var result = divModAny(this, v);
	        return {
	            quotient: result[0],
	            remainder: result[1]
	        };
	    };
	    SmallInteger.prototype.divmod = BigInteger.prototype.divmod;

	    BigInteger.prototype.divide = function (v) {
	        return divModAny(this, v)[0];
	    };
	    SmallInteger.prototype.over = SmallInteger.prototype.divide = BigInteger.prototype.over = BigInteger.prototype.divide;

	    BigInteger.prototype.mod = function (v) {
	        return divModAny(this, v)[1];
	    };
	    SmallInteger.prototype.remainder = SmallInteger.prototype.mod = BigInteger.prototype.remainder = BigInteger.prototype.mod;

	    BigInteger.prototype.pow = function (v) {
	        var n = parseValue(v),
	            a = this.value,
	            b = n.value,
	            value, x, y;
	        if (b === 0) return Integer[1];
	        if (a === 0) return Integer[0];
	        if (a === 1) return Integer[1];
	        if (a === -1) return n.isEven() ? Integer[1] : Integer[-1];
	        if (n.sign) {
	            return Integer[0];
	        }
	        if (!n.isSmall) throw new Error("The exponent " + n.toString() + " is too large.");
	        if (this.isSmall) {
	            if (isPrecise(value = Math.pow(a, b)))
	                return new SmallInteger(truncate(value));
	        }
	        x = this;
	        y = Integer[1];
	        while (true) {
	            if (b & 1 === 1) {
	                y = y.times(x);
	                --b;
	            }
	            if (b === 0) break;
	            b /= 2;
	            x = x.square();
	        }
	        return y;
	    };
	    SmallInteger.prototype.pow = BigInteger.prototype.pow;

	    BigInteger.prototype.modPow = function (exp, mod) {
	        exp = parseValue(exp);
	        mod = parseValue(mod);
	        if (mod.isZero()) throw new Error("Cannot take modPow with modulus 0");
	        var r = Integer[1],
	            base = this.mod(mod);
	        while (exp.isPositive()) {
	            if (base.isZero()) return Integer[0];
	            if (exp.isOdd()) r = r.multiply(base).mod(mod);
	            exp = exp.divide(2);
	            base = base.square().mod(mod);
	        }
	        return r;
	    };
	    SmallInteger.prototype.modPow = BigInteger.prototype.modPow;

	    function compareAbs(a, b) {
	        if (a.length !== b.length) {
	            return a.length > b.length ? 1 : -1;
	        }
	        for (var i = a.length - 1; i >= 0; i--) {
	            if (a[i] !== b[i]) return a[i] > b[i] ? 1 : -1;
	        }
	        return 0;
	    }

	    BigInteger.prototype.compareAbs = function (v) {
	        var n = parseValue(v),
	            a = this.value,
	            b = n.value;
	        if (n.isSmall) return 1;
	        return compareAbs(a, b);
	    };
	    SmallInteger.prototype.compareAbs = function (v) {
	        var n = parseValue(v),
	            a = Math.abs(this.value),
	            b = n.value;
	        if (n.isSmall) {
	            b = Math.abs(b);
	            return a === b ? 0 : a > b ? 1 : -1;
	        }
	        return -1;
	    };

	    BigInteger.prototype.compare = function (v) {
	        // See discussion about comparison with Infinity:
	        // https://github.com/peterolson/BigInteger.js/issues/61
	        if (v === Infinity) {
	            return -1;
	        }
	        if (v === -Infinity) {
	            return 1;
	        }

	        var n = parseValue(v),
	            a = this.value,
	            b = n.value;
	        if (this.sign !== n.sign) {
	            return n.sign ? 1 : -1;
	        }
	        if (n.isSmall) {
	            return this.sign ? -1 : 1;
	        }
	        return compareAbs(a, b) * (this.sign ? -1 : 1);
	    };
	    BigInteger.prototype.compareTo = BigInteger.prototype.compare;

	    SmallInteger.prototype.compare = function (v) {
	        if (v === Infinity) {
	            return -1;
	        }
	        if (v === -Infinity) {
	            return 1;
	        }

	        var n = parseValue(v),
	            a = this.value,
	            b = n.value;
	        if (n.isSmall) {
	            return a == b ? 0 : a > b ? 1 : -1;
	        }
	        if (a < 0 !== n.sign) {
	            return a < 0 ? -1 : 1;
	        }
	        return a < 0 ? 1 : -1;
	    };
	    SmallInteger.prototype.compareTo = SmallInteger.prototype.compare;

	    BigInteger.prototype.equals = function (v) {
	        return this.compare(v) === 0;
	    };
	    SmallInteger.prototype.eq = SmallInteger.prototype.equals = BigInteger.prototype.eq = BigInteger.prototype.equals;

	    BigInteger.prototype.notEquals = function (v) {
	        return this.compare(v) !== 0;
	    };
	    SmallInteger.prototype.neq = SmallInteger.prototype.notEquals = BigInteger.prototype.neq = BigInteger.prototype.notEquals;

	    BigInteger.prototype.greater = function (v) {
	        return this.compare(v) > 0;
	    };
	    SmallInteger.prototype.gt = SmallInteger.prototype.greater = BigInteger.prototype.gt = BigInteger.prototype.greater;

	    BigInteger.prototype.lesser = function (v) {
	        return this.compare(v) < 0;
	    };
	    SmallInteger.prototype.lt = SmallInteger.prototype.lesser = BigInteger.prototype.lt = BigInteger.prototype.lesser;

	    BigInteger.prototype.greaterOrEquals = function (v) {
	        return this.compare(v) >= 0;
	    };
	    SmallInteger.prototype.geq = SmallInteger.prototype.greaterOrEquals = BigInteger.prototype.geq = BigInteger.prototype.greaterOrEquals;

	    BigInteger.prototype.lesserOrEquals = function (v) {
	        return this.compare(v) <= 0;
	    };
	    SmallInteger.prototype.leq = SmallInteger.prototype.lesserOrEquals = BigInteger.prototype.leq = BigInteger.prototype.lesserOrEquals;

	    BigInteger.prototype.isEven = function () {
	        return (this.value[0] & 1) === 0;
	    };
	    SmallInteger.prototype.isEven = function () {
	        return (this.value & 1) === 0;
	    };

	    BigInteger.prototype.isOdd = function () {
	        return (this.value[0] & 1) === 1;
	    };
	    SmallInteger.prototype.isOdd = function () {
	        return (this.value & 1) === 1;
	    };

	    BigInteger.prototype.isPositive = function () {
	        return !this.sign;
	    };
	    SmallInteger.prototype.isPositive = function () {
	        return this.value > 0;
	    };

	    BigInteger.prototype.isNegative = function () {
	        return this.sign;
	    };
	    SmallInteger.prototype.isNegative = function () {
	        return this.value < 0;
	    };

	    BigInteger.prototype.isUnit = function () {
	        return false;
	    };
	    SmallInteger.prototype.isUnit = function () {
	        return Math.abs(this.value) === 1;
	    };

	    BigInteger.prototype.isZero = function () {
	        return false;
	    };
	    SmallInteger.prototype.isZero = function () {
	        return this.value === 0;
	    };
	    BigInteger.prototype.isDivisibleBy = function (v) {
	        var n = parseValue(v);
	        var value = n.value;
	        if (value === 0) return false;
	        if (value === 1) return true;
	        if (value === 2) return this.isEven();
	        return this.mod(n).equals(Integer[0]);
	    };
	    SmallInteger.prototype.isDivisibleBy = BigInteger.prototype.isDivisibleBy;

	    function isBasicPrime(v) {
	        var n = v.abs();
	        if (n.isUnit()) return false;
	        if (n.equals(2) || n.equals(3) || n.equals(5)) return true;
	        if (n.isEven() || n.isDivisibleBy(3) || n.isDivisibleBy(5)) return false;
	        if (n.lesser(25)) return true;
	        // we don't know if it's prime: let the other functions figure it out
	    }

	    BigInteger.prototype.isPrime = function () {
	        var isPrime = isBasicPrime(this);
	        if (isPrime !== undefined) return isPrime;
	        var n = this.abs(),
	            nPrev = n.prev();
	        var a = [2, 3, 5, 7, 11, 13, 17, 19],
	            b = nPrev,
	            d, t, i, x;
	        while (b.isEven()) b = b.divide(2);
	        for (i = 0; i < a.length; i++) {
	            x = bigInt(a[i]).modPow(b, n);
	            if (x.equals(Integer[1]) || x.equals(nPrev)) continue;
	            for (t = true, d = b; t && d.lesser(nPrev) ; d = d.multiply(2)) {
	                x = x.square().mod(n);
	                if (x.equals(nPrev)) t = false;
	            }
	            if (t) return false;
	        }
	        return true;
	    };
	    SmallInteger.prototype.isPrime = BigInteger.prototype.isPrime;

	    BigInteger.prototype.isProbablePrime = function (iterations) {
	        var isPrime = isBasicPrime(this);
	        if (isPrime !== undefined) return isPrime;
	        var n = this.abs();
	        var t = iterations === undefined ? 5 : iterations;
	        // use the Fermat primality test
	        for (var i = 0; i < t; i++) {
	            var a = bigInt.randBetween(2, n.minus(2));
	            if (!a.modPow(n.prev(), n).isUnit()) return false; // definitely composite
	        }
	        return true; // large chance of being prime
	    };
	    SmallInteger.prototype.isProbablePrime = BigInteger.prototype.isProbablePrime;

	    BigInteger.prototype.modInv = function (n) {
	        var t = bigInt.zero, newT = bigInt.one, r = parseValue(n), newR = this.abs(), q, lastT, lastR;
	        while (!newR.equals(bigInt.zero)) {
	            q = r.divide(newR);
	            lastT = t;
	            lastR = r;
	            t = newT;
	            r = newR;
	            newT = lastT.subtract(q.multiply(newT));
	            newR = lastR.subtract(q.multiply(newR));
	        }
	        if (!r.equals(1)) throw new Error(this.toString() + " and " + n.toString() + " are not co-prime");
	        if (t.compare(0) === -1) {
	            t = t.add(n);
	        }
	        if (this.isNegative()) {
	            return t.negate();
	        }
	        return t;
	    };

	    SmallInteger.prototype.modInv = BigInteger.prototype.modInv;

	    BigInteger.prototype.next = function () {
	        var value = this.value;
	        if (this.sign) {
	            return subtractSmall(value, 1, this.sign);
	        }
	        return new BigInteger(addSmall(value, 1), this.sign);
	    };
	    SmallInteger.prototype.next = function () {
	        var value = this.value;
	        if (value + 1 < MAX_INT) return new SmallInteger(value + 1);
	        return new BigInteger(MAX_INT_ARR, false);
	    };

	    BigInteger.prototype.prev = function () {
	        var value = this.value;
	        if (this.sign) {
	            return new BigInteger(addSmall(value, 1), true);
	        }
	        return subtractSmall(value, 1, this.sign);
	    };
	    SmallInteger.prototype.prev = function () {
	        var value = this.value;
	        if (value - 1 > -MAX_INT) return new SmallInteger(value - 1);
	        return new BigInteger(MAX_INT_ARR, true);
	    };

	    var powersOfTwo = [1];
	    while (2 * powersOfTwo[powersOfTwo.length - 1] <= BASE) powersOfTwo.push(2 * powersOfTwo[powersOfTwo.length - 1]);
	    var powers2Length = powersOfTwo.length, highestPower2 = powersOfTwo[powers2Length - 1];

	    function shift_isSmall(n) {
	        return ((typeof n === "number" || typeof n === "string") && +Math.abs(n) <= BASE) ||
	            (n instanceof BigInteger && n.value.length <= 1);
	    }

	    BigInteger.prototype.shiftLeft = function (n) {
	        if (!shift_isSmall(n)) {
	            throw new Error(String(n) + " is too large for shifting.");
	        }
	        n = +n;
	        if (n < 0) return this.shiftRight(-n);
	        var result = this;
	        while (n >= powers2Length) {
	            result = result.multiply(highestPower2);
	            n -= powers2Length - 1;
	        }
	        return result.multiply(powersOfTwo[n]);
	    };
	    SmallInteger.prototype.shiftLeft = BigInteger.prototype.shiftLeft;

	    BigInteger.prototype.shiftRight = function (n) {
	        var remQuo;
	        if (!shift_isSmall(n)) {
	            throw new Error(String(n) + " is too large for shifting.");
	        }
	        n = +n;
	        if (n < 0) return this.shiftLeft(-n);
	        var result = this;
	        while (n >= powers2Length) {
	            if (result.isZero()) return result;
	            remQuo = divModAny(result, highestPower2);
	            result = remQuo[1].isNegative() ? remQuo[0].prev() : remQuo[0];
	            n -= powers2Length - 1;
	        }
	        remQuo = divModAny(result, powersOfTwo[n]);
	        return remQuo[1].isNegative() ? remQuo[0].prev() : remQuo[0];
	    };
	    SmallInteger.prototype.shiftRight = BigInteger.prototype.shiftRight;

	    function bitwise(x, y, fn) {
	        y = parseValue(y);
	        var xSign = x.isNegative(), ySign = y.isNegative();
	        var xRem = xSign ? x.not() : x,
	            yRem = ySign ? y.not() : y;
	        var xDigit = 0, yDigit = 0;
	        var xDivMod = null, yDivMod = null;
	        var result = [];
	        while (!xRem.isZero() || !yRem.isZero()) {
	            xDivMod = divModAny(xRem, highestPower2);
	            xDigit = xDivMod[1].toJSNumber();
	            if (xSign) {
	                xDigit = highestPower2 - 1 - xDigit; // two's complement for negative numbers
	            }

	            yDivMod = divModAny(yRem, highestPower2);
	            yDigit = yDivMod[1].toJSNumber();
	            if (ySign) {
	                yDigit = highestPower2 - 1 - yDigit; // two's complement for negative numbers
	            }

	            xRem = xDivMod[0];
	            yRem = yDivMod[0];
	            result.push(fn(xDigit, yDigit));
	        }
	        var sum = fn(xSign ? 1 : 0, ySign ? 1 : 0) !== 0 ? bigInt(-1) : bigInt(0);
	        for (var i = result.length - 1; i >= 0; i -= 1) {
	            sum = sum.multiply(highestPower2).add(bigInt(result[i]));
	        }
	        return sum;
	    }

	    BigInteger.prototype.not = function () {
	        return this.negate().prev();
	    };
	    SmallInteger.prototype.not = BigInteger.prototype.not;

	    BigInteger.prototype.and = function (n) {
	        return bitwise(this, n, function (a, b) { return a & b; });
	    };
	    SmallInteger.prototype.and = BigInteger.prototype.and;

	    BigInteger.prototype.or = function (n) {
	        return bitwise(this, n, function (a, b) { return a | b; });
	    };
	    SmallInteger.prototype.or = BigInteger.prototype.or;

	    BigInteger.prototype.xor = function (n) {
	        return bitwise(this, n, function (a, b) { return a ^ b; });
	    };
	    SmallInteger.prototype.xor = BigInteger.prototype.xor;

	    var LOBMASK_I = 1 << 30, LOBMASK_BI = (BASE & -BASE) * (BASE & -BASE) | LOBMASK_I;
	    function roughLOB(n) { // get lowestOneBit (rough)
	        // SmallInteger: return Min(lowestOneBit(n), 1 << 30)
	        // BigInteger: return Min(lowestOneBit(n), 1 << 14) [BASE=1e7]
	        var v = n.value, x = typeof v === "number" ? v | LOBMASK_I : v[0] + v[1] * BASE | LOBMASK_BI;
	        return x & -x;
	    }

	    function max(a, b) {
	        a = parseValue(a);
	        b = parseValue(b);
	        return a.greater(b) ? a : b;
	    }
	    function min(a, b) {
	        a = parseValue(a);
	        b = parseValue(b);
	        return a.lesser(b) ? a : b;
	    }
	    function gcd(a, b) {
	        a = parseValue(a).abs();
	        b = parseValue(b).abs();
	        if (a.equals(b)) return a;
	        if (a.isZero()) return b;
	        if (b.isZero()) return a;
	        var c = Integer[1], d, t;
	        while (a.isEven() && b.isEven()) {
	            d = Math.min(roughLOB(a), roughLOB(b));
	            a = a.divide(d);
	            b = b.divide(d);
	            c = c.multiply(d);
	        }
	        while (a.isEven()) {
	            a = a.divide(roughLOB(a));
	        }
	        do {
	            while (b.isEven()) {
	                b = b.divide(roughLOB(b));
	            }
	            if (a.greater(b)) {
	                t = b; b = a; a = t;
	            }
	            b = b.subtract(a);
	        } while (!b.isZero());
	        return c.isUnit() ? a : a.multiply(c);
	    }
	    function lcm(a, b) {
	        a = parseValue(a).abs();
	        b = parseValue(b).abs();
	        return a.divide(gcd(a, b)).multiply(b);
	    }
	    function randBetween(a, b) {
	        a = parseValue(a);
	        b = parseValue(b);
	        var low = min(a, b), high = max(a, b);
	        var range = high.subtract(low).add(1);
	        if (range.isSmall) return low.add(Math.floor(Math.random() * range));
	        var length = range.value.length - 1;
	        var result = [], restricted = true;
	        for (var i = length; i >= 0; i--) {
	            var top = restricted ? range.value[i] : BASE;
	            var digit = truncate(Math.random() * top);
	            result.unshift(digit);
	            if (digit < top) restricted = false;
	        }
	        result = arrayToSmall(result);
	        return low.add(typeof result === "number" ? new SmallInteger(result) : new BigInteger(result, false));
	    }
	    var parseBase = function (text, base) {
	        var length = text.length;
			var i;
			var absBase = Math.abs(base);
			for(var i = 0; i < length; i++) {
				var c = text[i].toLowerCase();
				if(c === "-") continue;
				if(/[a-z0-9]/.test(c)) {
				    if(/[0-9]/.test(c) && +c >= absBase) {
						if(c === "1" && absBase === 1) continue;
	                    throw new Error(c + " is not a valid digit in base " + base + ".");
					} else if(c.charCodeAt(0) - 87 >= absBase) {
						throw new Error(c + " is not a valid digit in base " + base + ".");
					}
				}
			}
	        if (2 <= base && base <= 36) {
	            if (length <= LOG_MAX_INT / Math.log(base)) {
					var result = parseInt(text, base);
					if(isNaN(result)) {
						throw new Error(c + " is not a valid digit in base " + base + ".");
					}
	                return new SmallInteger(parseInt(text, base));
	            }
	        }
	        base = parseValue(base);
	        var digits = [];
	        var isNegative = text[0] === "-";
	        for (i = isNegative ? 1 : 0; i < text.length; i++) {
	            var c = text[i].toLowerCase(),
	                charCode = c.charCodeAt(0);
	            if (48 <= charCode && charCode <= 57) digits.push(parseValue(c));
	            else if (97 <= charCode && charCode <= 122) digits.push(parseValue(c.charCodeAt(0) - 87));
	            else if (c === "<") {
	                var start = i;
	                do { i++; } while (text[i] !== ">");
	                digits.push(parseValue(text.slice(start + 1, i)));
	            }
	            else throw new Error(c + " is not a valid character");
	        }
	        return parseBaseFromArray(digits, base, isNegative);
	    };

	    function parseBaseFromArray(digits, base, isNegative) {
	        var val = Integer[0], pow = Integer[1], i;
	        for (i = digits.length - 1; i >= 0; i--) {
	            val = val.add(digits[i].times(pow));
	            pow = pow.times(base);
	        }
	        return isNegative ? val.negate() : val;
	    }

	    function stringify(digit) {
	        var v = digit.value;
	        if (typeof v === "number") v = [v];
	        if (v.length === 1 && v[0] <= 35) {
	            return "0123456789abcdefghijklmnopqrstuvwxyz".charAt(v[0]);
	        }
	        return "<" + v + ">";
	    }
	    function toBase(n, base) {
	        base = bigInt(base);
	        if (base.isZero()) {
	            if (n.isZero()) return "0";
	            throw new Error("Cannot convert nonzero numbers to base 0.");
	        }
	        if (base.equals(-1)) {
	            if (n.isZero()) return "0";
	            if (n.isNegative()) return new Array(1 - n).join("10");
	            return "1" + new Array(+n).join("01");
	        }
	        var minusSign = "";
	        if (n.isNegative() && base.isPositive()) {
	            minusSign = "-";
	            n = n.abs();
	        }
	        if (base.equals(1)) {
	            if (n.isZero()) return "0";
	            return minusSign + new Array(+n + 1).join(1);
	        }
	        var out = [];
	        var left = n, divmod;
	        while (left.isNegative() || left.compareAbs(base) >= 0) {
	            divmod = left.divmod(base);
	            left = divmod.quotient;
	            var digit = divmod.remainder;
	            if (digit.isNegative()) {
	                digit = base.minus(digit).abs();
	                left = left.next();
	            }
	            out.push(stringify(digit));
	        }
	        out.push(stringify(left));
	        return minusSign + out.reverse().join("");
	    }

	    BigInteger.prototype.toString = function (radix) {
	        if (radix === undefined) radix = 10;
	        if (radix !== 10) return toBase(this, radix);
	        var v = this.value, l = v.length, str = String(v[--l]), zeros = "0000000", digit;
	        while (--l >= 0) {
	            digit = String(v[l]);
	            str += zeros.slice(digit.length) + digit;
	        }
	        var sign = this.sign ? "-" : "";
	        return sign + str;
	    };

	    SmallInteger.prototype.toString = function (radix) {
	        if (radix === undefined) radix = 10;
	        if (radix != 10) return toBase(this, radix);
	        return String(this.value);
	    };
	    BigInteger.prototype.toJSON = SmallInteger.prototype.toJSON = function() { return this.toString(); }

	    BigInteger.prototype.valueOf = function () {
	        return +this.toString();
	    };
	    BigInteger.prototype.toJSNumber = BigInteger.prototype.valueOf;

	    SmallInteger.prototype.valueOf = function () {
	        return this.value;
	    };
	    SmallInteger.prototype.toJSNumber = SmallInteger.prototype.valueOf;

	    function parseStringValue(v) {
	            if (isPrecise(+v)) {
	                var x = +v;
	                if (x === truncate(x))
	                    return new SmallInteger(x);
	                throw "Invalid integer: " + v;
	            }
	            var sign = v[0] === "-";
	            if (sign) v = v.slice(1);
	            var split = v.split(/e/i);
	            if (split.length > 2) throw new Error("Invalid integer: " + split.join("e"));
	            if (split.length === 2) {
	                var exp = split[1];
	                if (exp[0] === "+") exp = exp.slice(1);
	                exp = +exp;
	                if (exp !== truncate(exp) || !isPrecise(exp)) throw new Error("Invalid integer: " + exp + " is not a valid exponent.");
	                var text = split[0];
	                var decimalPlace = text.indexOf(".");
	                if (decimalPlace >= 0) {
	                    exp -= text.length - decimalPlace - 1;
	                    text = text.slice(0, decimalPlace) + text.slice(decimalPlace + 1);
	                }
	                if (exp < 0) throw new Error("Cannot include negative exponent part for integers");
	                text += (new Array(exp + 1)).join("0");
	                v = text;
	            }
	            var isValid = /^([0-9][0-9]*)$/.test(v);
	            if (!isValid) throw new Error("Invalid integer: " + v);
	            var r = [], max = v.length, l = LOG_BASE, min = max - l;
	            while (max > 0) {
	                r.push(+v.slice(min, max));
	                min -= l;
	                if (min < 0) min = 0;
	                max -= l;
	            }
	            trim(r);
	            return new BigInteger(r, sign);
	    }

	    function parseNumberValue(v) {
	        if (isPrecise(v)) {
	            if (v !== truncate(v)) throw new Error(v + " is not an integer.");
	            return new SmallInteger(v);
	        }
	        return parseStringValue(v.toString());
	    }

	    function parseValue(v) {
	        if (typeof v === "number") {
	            return parseNumberValue(v);
	        }
	        if (typeof v === "string") {
	            return parseStringValue(v);
	        }
	        return v;
	    }
	    // Pre-define numbers in range [-999,999]
	    for (var i = 0; i < 1000; i++) {
	        Integer[i] = new SmallInteger(i);
	        if (i > 0) Integer[-i] = new SmallInteger(-i);
	    }
	    // Backwards compatibility
	    Integer.one = Integer[1];
	    Integer.zero = Integer[0];
	    Integer.minusOne = Integer[-1];
	    Integer.max = max;
	    Integer.min = min;
	    Integer.gcd = gcd;
	    Integer.lcm = lcm;
	    Integer.isInstance = function (x) { return x instanceof BigInteger || x instanceof SmallInteger; };
	    Integer.randBetween = randBetween;

	    Integer.fromArray = function (digits, base, isNegative) {
	        return parseBaseFromArray(digits.map(parseValue), parseValue(base || 10), isNegative);
	    };

	    return Integer;
	})();

	// Node.js check
	if (typeof module !== "undefined" && module.hasOwnProperty("exports")) {
	    module.exports = bigInt;
	}

	//amd check
	if ( true ) {
	  !(__WEBPACK_AMD_DEFINE_ARRAY__ = [], __WEBPACK_AMD_DEFINE_RESULT__ = function() {
	    return bigInt;
	  }.apply(exports, __WEBPACK_AMD_DEFINE_ARRAY__), __WEBPACK_AMD_DEFINE_RESULT__ !== undefined && (module.exports = __WEBPACK_AMD_DEFINE_RESULT__));
	}

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(96)(module)))

/***/ }),
/* 136 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {// int64-buffer.js

	/*jshint -W018 */ // Confusing use of '!'.
	/*jshint -W030 */ // Expected an assignment or function call and instead saw an expression.
	/*jshint -W093 */ // Did you mean to return a conditional instead of an assignment?

	var Uint64BE, Int64BE, Uint64LE, Int64LE;

	!function(exports) {
	  // constants

	  var UNDEFINED = "undefined";
	  var BUFFER = (UNDEFINED !== typeof Buffer) && Buffer;
	  var UINT8ARRAY = (UNDEFINED !== typeof Uint8Array) && Uint8Array;
	  var ARRAYBUFFER = (UNDEFINED !== typeof ArrayBuffer) && ArrayBuffer;
	  var ZERO = [0, 0, 0, 0, 0, 0, 0, 0];
	  var isArray = Array.isArray || _isArray;
	  var BIT32 = 4294967296;
	  var BIT24 = 16777216;

	  // storage class

	  var storage; // Array;

	  // generate classes

	  Uint64BE = factory("Uint64BE", true, true);
	  Int64BE = factory("Int64BE", true, false);
	  Uint64LE = factory("Uint64LE", false, true);
	  Int64LE = factory("Int64LE", false, false);

	  // class factory

	  function factory(name, bigendian, unsigned) {
	    var posH = bigendian ? 0 : 4;
	    var posL = bigendian ? 4 : 0;
	    var pos0 = bigendian ? 0 : 3;
	    var pos1 = bigendian ? 1 : 2;
	    var pos2 = bigendian ? 2 : 1;
	    var pos3 = bigendian ? 3 : 0;
	    var fromPositive = bigendian ? fromPositiveBE : fromPositiveLE;
	    var fromNegative = bigendian ? fromNegativeBE : fromNegativeLE;
	    var proto = Int64.prototype;
	    var isName = "is" + name;
	    var _isInt64 = "_" + isName;

	    // properties
	    proto.buffer = void 0;
	    proto.offset = 0;
	    proto[_isInt64] = true;

	    // methods
	    proto.toNumber = toNumber;
	    proto.toString = toString;
	    proto.toJSON = toNumber;
	    proto.toArray = toArray;

	    // add .toBuffer() method only when Buffer available
	    if (BUFFER) proto.toBuffer = toBuffer;

	    // add .toArrayBuffer() method only when Uint8Array available
	    if (UINT8ARRAY) proto.toArrayBuffer = toArrayBuffer;

	    // isUint64BE, isInt64BE
	    Int64[isName] = isInt64;

	    // CommonJS
	    exports[name] = Int64;

	    return Int64;

	    // constructor
	    function Int64(buffer, offset, value, raddix) {
	      if (!(this instanceof Int64)) return new Int64(buffer, offset, value, raddix);
	      return init(this, buffer, offset, value, raddix);
	    }

	    // isUint64BE, isInt64BE
	    function isInt64(b) {
	      return !!(b && b[_isInt64]);
	    }

	    // initializer
	    function init(that, buffer, offset, value, raddix) {
	      if (UINT8ARRAY && ARRAYBUFFER) {
	        if (buffer instanceof ARRAYBUFFER) buffer = new UINT8ARRAY(buffer);
	        if (value instanceof ARRAYBUFFER) value = new UINT8ARRAY(value);
	      }

	      // Int64BE() style
	      if (!buffer && !offset && !value && !storage) {
	        // shortcut to initialize with zero
	        that.buffer = newArray(ZERO, 0);
	        return;
	      }

	      // Int64BE(value, raddix) style
	      if (!isValidBuffer(buffer, offset)) {
	        var _storage = storage || Array;
	        raddix = offset;
	        value = buffer;
	        offset = 0;
	        buffer = new _storage(8);
	      }

	      that.buffer = buffer;
	      that.offset = offset |= 0;

	      // Int64BE(buffer, offset) style
	      if (UNDEFINED === typeof value) return;

	      // Int64BE(buffer, offset, value, raddix) style
	      if ("string" === typeof value) {
	        fromString(buffer, offset, value, raddix || 10);
	      } else if (isValidBuffer(value, raddix)) {
	        fromArray(buffer, offset, value, raddix);
	      } else if ("number" === typeof raddix) {
	        writeInt32(buffer, offset + posH, value); // high
	        writeInt32(buffer, offset + posL, raddix); // low
	      } else if (value > 0) {
	        fromPositive(buffer, offset, value); // positive
	      } else if (value < 0) {
	        fromNegative(buffer, offset, value); // negative
	      } else {
	        fromArray(buffer, offset, ZERO, 0); // zero, NaN and others
	      }
	    }

	    function fromString(buffer, offset, str, raddix) {
	      var pos = 0;
	      var len = str.length;
	      var high = 0;
	      var low = 0;
	      if (str[0] === "-") pos++;
	      var sign = pos;
	      while (pos < len) {
	        var chr = parseInt(str[pos++], raddix);
	        if (!(chr >= 0)) break; // NaN
	        low = low * raddix + chr;
	        high = high * raddix + Math.floor(low / BIT32);
	        low %= BIT32;
	      }
	      if (sign) {
	        high = ~high;
	        if (low) {
	          low = BIT32 - low;
	        } else {
	          high++;
	        }
	      }
	      writeInt32(buffer, offset + posH, high);
	      writeInt32(buffer, offset + posL, low);
	    }

	    function toNumber() {
	      var buffer = this.buffer;
	      var offset = this.offset;
	      var high = readInt32(buffer, offset + posH);
	      var low = readInt32(buffer, offset + posL);
	      if (!unsigned) high |= 0; // a trick to get signed
	      return high ? (high * BIT32 + low) : low;
	    }

	    function toString(radix) {
	      var buffer = this.buffer;
	      var offset = this.offset;
	      var high = readInt32(buffer, offset + posH);
	      var low = readInt32(buffer, offset + posL);
	      var str = "";
	      var sign = !unsigned && (high & 0x80000000);
	      if (sign) {
	        high = ~high;
	        low = BIT32 - low;
	      }
	      radix = radix || 10;
	      while (1) {
	        var mod = (high % radix) * BIT32 + low;
	        high = Math.floor(high / radix);
	        low = Math.floor(mod / radix);
	        str = (mod % radix).toString(radix) + str;
	        if (!high && !low) break;
	      }
	      if (sign) {
	        str = "-" + str;
	      }
	      return str;
	    }

	    function writeInt32(buffer, offset, value) {
	      buffer[offset + pos3] = value & 255;
	      value = value >> 8;
	      buffer[offset + pos2] = value & 255;
	      value = value >> 8;
	      buffer[offset + pos1] = value & 255;
	      value = value >> 8;
	      buffer[offset + pos0] = value & 255;
	    }

	    function readInt32(buffer, offset) {
	      return (buffer[offset + pos0] * BIT24) +
	        (buffer[offset + pos1] << 16) +
	        (buffer[offset + pos2] << 8) +
	        buffer[offset + pos3];
	    }
	  }

	  function toArray(raw) {
	    var buffer = this.buffer;
	    var offset = this.offset;
	    storage = null; // Array
	    if (raw !== false && offset === 0 && buffer.length === 8 && isArray(buffer)) return buffer;
	    return newArray(buffer, offset);
	  }

	  function toBuffer(raw) {
	    var buffer = this.buffer;
	    var offset = this.offset;
	    storage = BUFFER;
	    if (raw !== false && offset === 0 && buffer.length === 8 && Buffer.isBuffer(buffer)) return buffer;
	    var dest = new BUFFER(8);
	    fromArray(dest, 0, buffer, offset);
	    return dest;
	  }

	  function toArrayBuffer(raw) {
	    var buffer = this.buffer;
	    var offset = this.offset;
	    var arrbuf = buffer.buffer;
	    storage = UINT8ARRAY;
	    if (raw !== false && offset === 0 && (arrbuf instanceof ARRAYBUFFER) && arrbuf.byteLength === 8) return arrbuf;
	    var dest = new UINT8ARRAY(8);
	    fromArray(dest, 0, buffer, offset);
	    return dest.buffer;
	  }

	  function isValidBuffer(buffer, offset) {
	    var len = buffer && buffer.length;
	    offset |= 0;
	    return len && (offset + 8 <= len) && ("string" !== typeof buffer[offset]);
	  }

	  function fromArray(destbuf, destoff, srcbuf, srcoff) {
	    destoff |= 0;
	    srcoff |= 0;
	    for (var i = 0; i < 8; i++) {
	      destbuf[destoff++] = srcbuf[srcoff++] & 255;
	    }
	  }

	  function newArray(buffer, offset) {
	    return Array.prototype.slice.call(buffer, offset, offset + 8);
	  }

	  function fromPositiveBE(buffer, offset, value) {
	    var pos = offset + 8;
	    while (pos > offset) {
	      buffer[--pos] = value & 255;
	      value /= 256;
	    }
	  }

	  function fromNegativeBE(buffer, offset, value) {
	    var pos = offset + 8;
	    value++;
	    while (pos > offset) {
	      buffer[--pos] = ((-value) & 255) ^ 255;
	      value /= 256;
	    }
	  }

	  function fromPositiveLE(buffer, offset, value) {
	    var end = offset + 8;
	    while (offset < end) {
	      buffer[offset++] = value & 255;
	      value /= 256;
	    }
	  }

	  function fromNegativeLE(buffer, offset, value) {
	    var end = offset + 8;
	    value++;
	    while (offset < end) {
	      buffer[offset++] = ((-value) & 255) ^ 255;
	      value /= 256;
	    }
	  }

	  // https://github.com/retrofox/is-array
	  function _isArray(val) {
	    return !!val && "[object Array]" == Object.prototype.toString.call(val);
	  }

	}(typeof exports === 'object' && typeof exports.nodeName !== 'string' ? exports : (this || {}));

	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 137 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4);
	var ConsistentHashRing = __webpack_require__(138).ConsistentHashRing;

	//SCRIPT START
	var HashPartitionResolver = Base.defineClass(
	    /**
	     * HashPartitionResolver implements partitioning based on the value of a hash function, 
	     * allowing you to evenly distribute requests and data across a number of partitions for
	     * the Azure Cosmos DB database service.
	     * @class HashPartitionResolver
	     * @param {string | function} partitionKeyExtractor   - If partitionKeyExtractor is a string, it should be the name of the property in the document to execute the hashing on.
	     *                                                      If partitionKeyExtractor is a function, it should be a function to extract the partition key from an object.
	     **/
	    function (partitionKeyExtractor, collectionLinks, options) {
	        HashPartitionResolver._throwIfInvalidPartitionKeyExtractor(partitionKeyExtractor);
	        HashPartitionResolver._throwIfInvalidCollectionLinks(collectionLinks);
	        this.partitionKeyExtractor = partitionKeyExtractor;
	        
	        options = options || {};
	        this.consistentHashRing = new ConsistentHashRing(collectionLinks, options);
	        this.collectionLinks = collectionLinks;
	    }, {
	        /**
	         * Extracts the partition key from the specified document using the partitionKeyExtractor
	         * @memberof HashPartitionResolver
	         * @instance
	         * @param {object} document - The document from which to extract the partition key.
	         * @returns {object} 
	         **/
	        getPartitionKey: function (document) {
	            return (typeof this.partitionKeyExtractor === "string")
	                ? document[this.partitionKeyExtractor]
	                : this.partitionKeyExtractor(document);
	        },
	        /**
	         * Given a partition key, returns a list of collection links to read from.
	         * @memberof HashPartitionResolver
	         * @instance
	         * @param {any} partitionKey - The partition key used to determine the target collection for query
	         **/
	        resolveForRead: function (partitionKey) {
	            if (partitionKey === undefined || partitionKey === null) {
	                return this.collectionLinks;
	            }

	            return [this._resolve(partitionKey)];            
	        },
	        /**
	         * Given a partition key, returns the correct collection link for creating a document.
	         * @memberof HashPartitionResolver
	         * @instance
	         * @param {any} partitionKey - The partition key used to determine the target collection for create
	         * @returns {string}         - The target collection link that will be used for document creation.
	         **/
	        resolveForCreate: function (partitionKey) {
	            return this._resolve(partitionKey);
	        },
	        /** @ignore */
	        _resolve: function (partitionKey) {
	            HashPartitionResolver._throwIfInvalidPartitionKey(partitionKey);
	            return this.consistentHashRing.getNode(partitionKey);
	        }
	    }, {
	        /** @ignore */
	        _throwIfInvalidPartitionKeyExtractor: function (partitionKeyExtractor) {
	            if (partitionKeyExtractor === undefined || partitionKeyExtractor === null) {
	                throw new Error("partitionKeyExtractor cannot be null or undefined");
	            }
	            
	            if (typeof partitionKeyExtractor !== "string" && typeof partitionKeyExtractor !== "function") {
	                throw new Error("partitionKeyExtractor must be either a 'string' or a 'function'");
	            }
	        },
	        /** @ignore */
	        _throwIfInvalidPartitionKey: function (partitionKey) {
	            var partitionKeyType = typeof partitionKey;
	            if (partitionKeyType !== "string") {
	                throw new Error("partitionKey must be a 'string'");
	            }
	        },
	        /** @ignore */
	        _throwIfInvalidCollectionLinks: function (collectionLinks) {
	            if (!Array.isArray(collectionLinks)) {
	                throw new Error("collectionLinks must be an array.");
	            }
	            
	            if (collectionLinks.some(function (collectionLink) { return !Base._isValidCollectionLink(collectionLink); })) {
	                throw new Error("All elements of collectionLinks must be collection links.");
	            }
	        }
	    });

	//SCRIPT END

	if (true) {
	    exports.HashPartitionResolver = HashPartitionResolver;
	}

/***/ }),
/* 138 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4);
	var MurmurHash = __webpack_require__(139).MurmurHash;

	//SCRIPT START
	var ConsistentHashRing = Base.defineClass(
	    /**
	     * Initializes a new instance of the ConsistentHashRing
	     * @param {string[]} nodes - Array of collection links
	     * @param {object} options - Options to initialize the ConsistentHashRing
	     * @param {function} options.computeHash - Function to compute the hash for a given link or partition key
	     * @param {function} options.numberOfVirtualNodesPerCollection - Number of points in the ring to assign to each collection link
	     */
	    function (nodes, options) {
	        ConsistentHashRing._throwIfInvalidNodes(nodes);
	        
	        options = options || {};
	        options.numberOfVirtualNodesPerCollection = options.numberOfVirtualNodesPerCollection || 128;
	        options.computeHash = options.computeHash || MurmurHash.hash;
	        
	        this._computeHash = options.computeHash;
	        this._partitions = ConsistentHashRing._constructPartitions(nodes, options.numberOfVirtualNodesPerCollection, options.computeHash);
	    }, {
	        getNode: function (key) {
	            var hash = this._computeHash(key);
	            var partition = ConsistentHashRing._search(this._partitions, hash);            
	            return this._partitions[partition].node;
	        }
	    },{
	        /** @ignore */
	        _constructPartitions: function (nodes, partitionsPerNode, computeHashFunction) {
	            var partitions = new Array();
	            nodes.forEach(function (node) {
	                var hashValue = computeHashFunction(node);
	                for (var j = 0; j < partitionsPerNode; j++) {
	                    partitions.push({
	                        hashValue: hashValue, 
	                        node: node
	                    });
	                    
	                    hashValue = computeHashFunction(hashValue);
	                }
	            });
	            
	            partitions.sort(function (x, y) {
	                return ConsistentHashRing._compareHashes(x.hashValue, y.hashValue);
	            });
	            return partitions;
	        },
	        /** @ignore */
	        _compareHashes: function (x, y) {
	            if (x < y) return -1;
	            if (x > y) return 1;
	            return 0;
	        },
	        /** @ignore */
	        _search: function (partitions, hashValue) {
	            for (var i = 0; i < partitions.length - 1; i++) {
	                if (hashValue >= partitions[i].hashValue && hashValue < partitions[i + 1].hashValue) {
	                    return i;
	                }
	            }
	            
	            return partitions.length - 1;
	        },
	        /** @ignore */
	        _throwIfInvalidNodes: function (nodes) {
	            if (Array.isArray(nodes)) {
	                return;
	            }
	            
	            throw new Error("Invalid argument: 'nodes' has to be an array.");
	        }
	    }
	        
	);

	//SCRIPT END

	if (true) {
	    exports.ConsistentHashRing = ConsistentHashRing;
	}

/***/ }),
/* 139 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4);

	//SCRIPT START
	var MurmurHash = Base.defineClass(
	    undefined, 
	    undefined,
	    {
	        /**
	         * Hashes a string, a unsigned 32-bit integer, or a Buffer into a new unsigned 32-bit integer that represents the output hash.
	         * @param {string, number of Buffer} key  - The preimage of the hash
	         * @param {number} seed                   - Optional value used to initialize the hash generator
	         * @returns {} 
	         */
	        hash: function (key, seed) {
	            key = key || '';
	            seed = seed || 0;
	            
	            MurmurHash._throwIfInvalidKey(key);
	            MurmurHash._throwIfInvalidSeed(seed);
	            
	            var buffer;
	            if (typeof key === "string") {
	                buffer = MurmurHash._getBufferFromString(key);
	            }
	            else if (typeof key === "number") {
	                buffer = MurmurHash._getBufferFromNumber(key);
	            }
	            else {
	                buffer = key;
	            }
	            
	            return MurmurHash._hashBytes(buffer, seed);
	        },
	        /** @ignore */
	        _throwIfInvalidKey: function (key) {
	            if (key instanceof Buffer) {
	                return;
	            }
	            
	            if (typeof key === "string") {
	                return;
	            }
	            
	            if (typeof key === "number") {
	                return;
	            }
	            
	            throw new Error("Invalid argument: 'key' has to be a Buffer, string, or number.");
	        },
	        /** @ignore */
	        _throwIfInvalidSeed: function (seed) {
	            if (isNaN(seed)) {
	                throw new Error("Invalid argument: 'seed' is not and cannot be converted to a number.");
	            }
	        },
	        /** @ignore */
	        _getBufferFromString: function (key) {
	            var buffer = new Buffer(key);
	            return buffer;
	        },
	        /** @ignore */
	        _getBufferFromNumber: function (i) {
	            i = i >>> 0;
	            
	            var buffer = new Uint8Array([
	                i >>> 0,
	                i >>> 8,
	                i >>> 16,
	                i >>> 24
	            ]);

	            return buffer;
	        },
	        /** @ignore */
	        _hashBytes: function (bytes, seed) {
	            var c1 = 0xcc9e2d51;
	            var c2 = 0x1b873593;
	            
	            var h1 = seed;
	            var reader = new Uint32Array(bytes);
	            {
	                for (var i = 0; i < bytes.length - 3; i += 4) {
	                    var k1 = MurmurHash._readUInt32(reader, i);
	                    
	                    k1 = MurmurHash._multiply(k1, c1);
	                    k1 = MurmurHash._rotateLeft(k1, 15);
	                    k1 = MurmurHash._multiply(k1, c2);
	                    
	                    h1 ^= k1;
	                    h1 = MurmurHash._rotateLeft(h1, 13);
	                    h1 = MurmurHash._multiply(h1, 5) + 0xe6546b64;
	                }
	            }
	            
	            var k = 0;
	            switch (bytes.length & 3) {
	                case 3:
	                    k ^= reader[i + 2] << 16;
	                    k ^= reader[i + 1] << 8;
	                    k ^= reader[i];
	                    break;

	                case 2:
	                    k ^= reader[i + 1] << 8;
	                    k ^= reader[i];
	                    break;

	                case 1:
	                    k ^= reader[i];
	                    break;
	            }
	            
	            k = MurmurHash._multiply(k, c1);
	            k = MurmurHash._rotateLeft(k, 15);
	            k = MurmurHash._multiply(k, c2);
	            
	            h1 ^= k;
	            h1 ^= bytes.length;
	            h1 ^= h1 >>> 16;
	            h1 = MurmurHash._multiply(h1, 0x85ebca6b);
	            h1 ^= h1 >>> 13;
	            h1 = MurmurHash._multiply(h1, 0xc2b2ae35);
	            h1 ^= h1 >>> 16;
	            
	            return h1 >>> 0;
	        },
	        /** @ignore */
	        _rotateLeft: function (n, numBits) {
	            return (n << numBits) | (n >>> (32 - numBits));
	        },
	        /** @ignore */
	        _multiply: function (m, n) {
	            return ((m & 0xffff) * n) + ((((m >>> 16) * n) & 0xffff) << 16);
	        },
	        /** @ignore */
	        _readUInt32: function (uintArray, i) {
	            return (uintArray[i]) | (uintArray[i + 1] << 8) | (uintArray[i + 2] << 16) | (uintArray[i + 3] << 24) >>> 0;
	        }
	    });

	//SCRIPT END

	if (true) {
	    exports.MurmurHash = MurmurHash;
	}
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(5).Buffer))

/***/ }),
/* 140 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4);

	//SCRIPT START
	var Range = Base.defineClass(
	    /**
	     * Represents a range object used by the RangePartitionResolver in the Azure Cosmos DB database service.
	     * @class Range
	     * @param {object} options                   - The Range constructor options.
	     * @param {any} options.low                  - The low value in the range.
	     * @param {any} options.high                 - The high value in the range.
	     **/
	    function(options) {
	        if (options === undefined) {
	            options = {};
	        }
	        if (options === null) {
	            throw new Error("Invalid argument: 'options' is null");
	        }
	        if (typeof options !== "object") {
	            throw new Error("Invalid argument: 'options' is not an object");
	        }
	        if (options.high === undefined) {
	            options.high = options.low;
	        }
	        this.low = options.low;
	        this.high = options.high;
	        Object.freeze(this);
	    },
	    {
	        /** @ignore */
	        _compare: function (x, y, compareFunction) {
	            // Same semantics as Array.sort
	            // http://www.ecma-international.org/ecma-262/6.0/#sec-sortcompare
	            if (x === undefined && y === undefined)
	                return 0;
	            if (x === undefined)
	                return 1;
	            if (y === undefined)
	                return -1;
	            if (compareFunction !== undefined) {
	                var v = Number(compareFunction(x, y));
	                if (v === NaN)
	                    return 0;
	                return v;
	            }
	            var xString = String(x);
	            var yString = String(y);
	            if (xString < yString)
	                return -1;
	            if (xString > yString)
	                return 1;
	            return 0;
	        },

	        /** @ignore */
	        _contains: function (other, compareFunction) {
	            if (Range._isRange(other)) {
	                return this._containsRange(other, compareFunction);
	            }
	            else {
	                return this._containsPoint(other, compareFunction);
	            }
	        },

	        /** @ignore */
	        _containsPoint: function (point, compareFunction) {
	            if (this._compare(point, this.low, compareFunction) >= 0 && this._compare(point, this.high, compareFunction) <= 0) {
	                return true;
	            }
	            return false;
	        },

	        /** @ignore */
	        _containsRange: function (other, compareFunction) {
	            if (this._compare(other.low, this.low, compareFunction) >= 0 && this._compare(other.high, this.high, compareFunction) <= 0) {
	                return true;
	            }
	            return false;
	        },

	        /** @ignore */
	        _intersect: function (other, compareFunction) {
	            if (other === undefined || other === null) {
	                throw new Error("Invalid Argument: 'other' is undefined or null");
	            }
	            var maxLow = this._compare(this.low, other.low, compareFunction) >= 0 ? this.low : other.low;
	            var minHigh = this._compare(this.high, other.high, compareFunction) <= 0 ? this.high : other.high;
	            if (this._compare(maxLow, minHigh, compareFunction) <= 0) {
	                return true;
	            }
	            return false;
	        },

	        /** @ignore */
	        _toString: function () {
	            return String(this.low) + "," + String(this.high);
	        }
	    },
	    {
	        /** @ignore */
	        _isRange: function (pointOrRange) {
	            if (pointOrRange === undefined) {
	                return false;
	            }
	            if (pointOrRange === null) {
	                return false;
	            }
	            if (typeof pointOrRange !== "object") {
	                return false;
	            }
	            return ("low" in pointOrRange && "high" in pointOrRange);
	        }
	    }
	);

	var RangePartitionResolver = Base.defineClass(
	    /**
	     * RangePartitionResolver implements partitioning using a partition map of ranges of values to a collection link in the Azure Cosmos DB database service.
	     * @class RangePartitionResolver
	     * @param {string | function} partitionKeyExtractor   - If partitionKeyExtractor is a string, it should be the name of the property in the document to execute the hashing on.
	     *                                                      If partitionKeyExtractor is a function, it should be a function to extract the partition key from an object.
	     * @param {Array} partitionKeyMap                     - The map from Range to collection link that is used for partitioning requests.
	     * @param {function} compareFunction                  - Optional function that accepts two arguments x and y and returns a negative value if x < y, zero if x = y, or a positive value if x > y.
	     **/
	    function(partitionKeyExtractor, partitionKeyMap, compareFunction) {
	        if (partitionKeyExtractor === undefined || partitionKeyExtractor === null) {
	            throw new Error("partitionKeyExtractor cannot be null or undefined");
	        }
	        if (typeof partitionKeyExtractor !== "string" && typeof partitionKeyExtractor !== "function") {
	            throw new Error("partitionKeyExtractor must be either a 'string' or a 'function'");
	        }
	        if (partitionKeyMap === undefined || partitionKeyMap === null) {
	            throw new Error("partitionKeyMap cannot be null or undefined");
	        }
	        if (!(Array.isArray(partitionKeyMap))) {
	            throw new Error("partitionKeyMap has to be an Array");
	        }
	        var allMapEntriesAreValid = partitionKeyMap.every(function (mapEntry) {
	            if ((mapEntry === undefined) || mapEntry === null) {
	                return false;
	            }
	            if (mapEntry.range === undefined) {
	                return false;
	            }
	            if (!(mapEntry.range instanceof Range)) {
	                return false;
	            }
	            if (mapEntry.link === undefined) {
	                return false;
	            }
	            if (typeof mapEntry.link !== "string") {
	                return false;
	            }
	            return true;
	        });
	        if (!allMapEntriesAreValid) {
	            throw new Error("All partitionKeyMap entries have to be a tuple {range: Range, link: string }");
	        }
	        if (compareFunction !== undefined && typeof compareFunction !== "function") {
	            throw new Error("Invalid argument: 'compareFunction' is not a function");
	        }

	        this.partitionKeyExtractor = partitionKeyExtractor;
	        this.partitionKeyMap = partitionKeyMap;
	        this.compareFunction = compareFunction;
	    }, {
	        /**
	         * Extracts the partition key from the specified document using the partitionKeyExtractor
	         * @memberof RangePartitionResolver
	         * @instance
	         * @param {object} document - The document from which to extract the partition key.
	         * @returns {}
	         **/
	        getPartitionKey: function (document) {
	            if (typeof this.partitionKeyExtractor === "string") {
	                return document[this.partitionKeyExtractor];
	            }
	            if (typeof this.partitionKeyExtractor === "function") {
	                return this.partitionKeyExtractor(document);
	            }
	            throw new Error("Unable to extract partition key from document. Ensure PartitionKeyExtractor is a valid function or property name.");
	        },

	        /**
	         * Given a partition key, returns the correct collection link for creating a document using the range partition map.
	         * @memberof RangePartitionResolver
	         * @instance
	         * @param {any} partitionKey - The partition key used to determine the target collection for create
	         * @returns {string}         - The target collection link that will be used for document creation.
	         **/
	        resolveForCreate: function (partitionKey) {
	            var range = new Range({ low: partitionKey });
	            var mapEntry = this._getFirstContainingMapEntryOrNull(range);
	            if (mapEntry !== undefined && mapEntry !== null) {
	                return mapEntry.link;
	            }
	            throw new Error("Invalid operation: A containing range for '" + range._toString() + "' doesn't exist in the partition map.");
	        },

	        /**
	         * Given a partition key, returns a list of collection links to read from using the range partition map.
	         * @memberof RangePartitionResolver
	         * @instance
	         * @param {any} partitionKey - The partition key used to determine the target collection for query
	         * @returns {string[]}         - The list of target collection links.
	         **/
	        resolveForRead: function (partitionKey) {
	            if (partitionKey === undefined || partitionKey === null) {
	                return this.partitionKeyMap.map(function (i) { return i.link; });
	            }
	            else {
	                return this._getIntersectingMapEntries(partitionKey).map(function (i) { return i.link; });
	            }
	        },

	        /** @ignore */
	        _getFirstContainingMapEntryOrNull: function (point) {
	            var _this = this;
	            var containingMapEntries = this.partitionKeyMap.filter(function (p) { return p.range !== undefined && p.range._contains(point, _this.compareFunction); });
	            if (containingMapEntries && containingMapEntries.length > 0) {
	                return containingMapEntries[0];
	            }
	            return null;
	        },

	        /** @ignore */
	        _getIntersectingMapEntries: function (partitionKey) {
	            var _this = this;
	            var partitionKeys = (partitionKey instanceof Array) ? partitionKey : [partitionKey];
	            var ranges = partitionKeys.map(function (p) { return Range._isRange(p) ? p : new Range({ low: p }); });
	            var result = new Array();
	            ranges.forEach(function (range) {
	                result = result.concat(_this.partitionKeyMap.filter(function (entry) {
	                    return entry.range._intersect(range, _this.compareFunction);
	                }));
	            });
	            return result;
	        }
	    }
	);
	//SCRIPT END

	if (true) {
	    exports.Range = Range;
	    exports.RangePartitionResolver = RangePartitionResolver;
	}

/***/ }),
/* 141 */
/***/ (function(module, exports, __webpack_require__) {

	/*
	The MIT License (MIT)
	Copyright (c) 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
	*/

	"use strict";

	var Base = __webpack_require__(4)
	    , Constants = __webpack_require__(69)
	    , Helper = __webpack_require__(132).Helper;


	//SCRIPT START
	var UriFactory = Base.defineClass(

	    /**************************CONSTRUCTORS**************************/
	    undefined,

	    /************************INSTANCE MEMBERS************************/
	    undefined,

	    /*************************STATIC METHODS*************************/
	    {
	        /**
	        * Given a database id, this creates a database link.
	        * @param {string} databaseId -The database id
	        * @returns {string}          -A database link in the format of dbs/{0} with {0} being a Uri escaped version of the databaseId
	        * @description Would be used when creating or deleting a DocumentCollection or a User in Azure Cosmos DB database service
	        */
	        createDatabaseUri: function (databaseId) {
	            databaseId = Helper.trimSlashFromLeftAndRight(databaseId);
	            Helper.validateResourceId(databaseId);

	            return Constants.Path.DatabasesPathSegment + "/" +
	                databaseId;
	        },

	        /**
	        * Given a database and collection id, this creates a collection link.
	        * @param {string} databaseId        -The database id
	        * @param {string} collectionId      -The collection id
	        * @returns {string}                 A collection link in the format of dbs/{0}/colls/{1} with {0} being a Uri escaped version of the databaseId and {1} being collectionId
	        * @description Would be used when updating or deleting a DocumentCollection, creating a Document, a StoredProcedure, a Trigger, a UserDefinedFunction, or when executing a query with CreateDocumentQuery in Azure Cosmos DB database service.
	        */
	        createDocumentCollectionUri: function (databaseId, collectionId) {
	            collectionId = Helper.trimSlashFromLeftAndRight(collectionId);
	            Helper.validateResourceId(collectionId);

	            return this.createDatabaseUri(databaseId) + "/" +
	                Constants.Path.CollectionsPathSegment + "/" +
	                collectionId;
	        },

	        /**
	        * Given a database and user id, this creates a user link.
	        * @param {string} databaseId        -The database id
	        * @param {string} userId            -The user id
	        * @returns {string}                 A user link in the format of dbs/{0}/users/{1} with {0} being a Uri escaped version of the databaseId and {1} being userId
	        * @description Would be used when creating a Permission, or when replacing or deleting a User in Azure Cosmos DB database service
	        */
	        createUserUri: function (databaseId, userId) {
	            userId = Helper.trimSlashFromLeftAndRight(userId);
	            Helper.validateResourceId(userId);

	            return this.createDatabaseUri(databaseId) + "/" +
	                Constants.Path.UsersPathSegment + "/" +
	                userId;
	        },

	        /**
	        * Given a database and collection id, this creates a collection link.
	        * @param {string} databaseId        -The database id
	        * @param {string} collectionId      -The collection id
	        * @param {string} documentId        -The document id
	        * @returns {string}                 -A document link in the format of dbs/{0}/colls/{1}/docs/{2} with {0} being a Uri escaped version of the databaseId, {1} being collectionId and {2} being the documentId
	        * @description Would be used when creating an Attachment, or when replacing or deleting a Document in Azure Cosmos DB database service
	        */
	        createDocumentUri: function (databaseId, collectionId, documentId) {
	            documentId = Helper.trimSlashFromLeftAndRight(documentId);
	            Helper.validateResourceId(documentId);

	            return this.createDocumentCollectionUri(databaseId, collectionId) + "/" +
	                Constants.Path.DocumentsPathSegment + "/" +
	                documentId;
	        },

	        /**
	        * Given a database, collection and document id, this creates a document link.
	        * @param {string} databaseId    -The database Id
	        * @param {string} userId        -The user Id
	        * @param {string} permissionId  - The permissionId
	        * @returns {string} A permission link in the format of dbs/{0}/users/{1}/permissions/{2} with {0} being a Uri escaped version of the databaseId, {1} being userId and {2} being permissionId
	        * @description Would be used when replacing or deleting a Permission in Azure Cosmos DB database service.
	        */
	        createPermissionUri: function (databaseId, userId, permissionId) {
	            permissionId = Helper.trimSlashFromLeftAndRight(permissionId);
	            Helper.validateResourceId(permissionId);
	            
	            return this.createUserUri(databaseId, userId) + "/" +
	                Constants.Path.PermissionsPathSegment + "/" +
	                permissionId;
	        },

	        /**
	        * Given a database, collection and stored proc id, this creates a stored proc link.
	        * @param {string} databaseId        -The database Id
	        * @param {string} collectionId      -The collection Id
	        * @param {string} storedProcedureId -The stored procedure Id
	        * @returns {string}                 -A stored procedure link in the format of dbs/{0}/colls/{1}/sprocs/{2} with {0} being a Uri escaped version of the databaseId, {1} being collectionId and {2} being the storedProcedureId
	        * @description Would be used when replacing, executing, or deleting a StoredProcedure in Azure Cosmos DB database service.
	        */
	        createStoredProcedureUri: function (databaseId, collectionId, storedProcedureId) {
	            storedProcedureId = Helper.trimSlashFromLeftAndRight(storedProcedureId);
	            Helper.validateResourceId(storedProcedureId);

	            return this.createDocumentCollectionUri(databaseId, collectionId) + "/" +
	                Constants.Path.StoredProceduresPathSegment + "/" +
	                storedProcedureId;
	        },

	        /**
	        * @summary Given a database, collection and trigger id, this creates a trigger link.
	        * @param {string} databaseId        -The database Id
	        * @param {string} collectionId      -The collection Id
	        * @param {string} triggerId         -The trigger Id
	        * @returns {string}                 -A trigger link in the format of dbs/{0}/colls/{1}/triggers/{2} with {0} being a Uri escaped version of the databaseId, {1} being collectionId and {2} being the triggerId
	        * @description Would be used when replacing, executing, or deleting a Trigger in Azure Cosmos DB database service
	        */
	        createTriggerUri: function (databaseId, collectionId, triggerId) {
	            triggerId = Helper.trimSlashFromLeftAndRight(triggerId);
	            Helper.validateResourceId(triggerId);

	            return this.createDocumentCollectionUri(databaseId, collectionId) + "/" +
	                Constants.Path.TriggersPathSegment + "/" +
	                triggerId;
	        },

	        /**
	        * @summary Given a database, collection and udf id, this creates a udf link.
	        * @param {string} databaseId        -The database Id
	        * @param {string} collectionId      -The collection Id
	        * @param {string} udfId             -The User Defined Function Id
	        * @returns {string}                 -A udf link in the format of dbs/{0}/colls/{1}/udfs/{2} with {0} being a Uri escaped version of the databaseId, {1} being collectionId and {2} being the udfId
	        * @description Would be used when replacing, executing, or deleting a UserDefinedFunction in Azure Cosmos DB database service
	        */
	        createUserDefinedFunctionUri: function (databaseId, collectionId, udfId) {
	            udfId = Helper.trimSlashFromLeftAndRight(udfId);
	            Helper.validateResourceId(udfId);

	            return this.createDocumentCollectionUri(databaseId, collectionId) + "/" +
	                Constants.Path.UserDefinedFunctionsPathSegment + "/" +
	                udfId;
	        },

	        /**
	        * @summary Given a database, collection and conflict id, this creates a conflict link.
	        * @param {string} databaseId        -The database Id
	        * @param {string} collectionId      -The collection Id
	        * @param {string} conflictId        -The conflict Id
	        * @returns {string}                 -A conflict link in the format of dbs/{0}/colls/{1}/conflicts/{2} with {0} being a Uri escaped version of the databaseId, {1} being collectionId and {2} being the conflictId
	        * @description Would be used when creating a Conflict in Azure Cosmos DB database service.
	        */
	        createConflictUri: function (databaseId, collectionId, conflictId) {
	            conflictId = Helper.trimSlashFromLeftAndRight(conflictId);
	            Helper.validateResourceId(conflictId);

	            return this.createDocumentCollectionUri(databaseId, collectionId) + "/" +
	                Constants.Path.ConflictsPathSegment + "/" +
	                conflictId;
	        },

	        /**
	         * @summary Given a database, collection and conflict id, this creates a conflict link.
	         * @param {string} databaseId        -The database Id
	         * @param {string} collectionId      -The collection Id
	         * @param {string} documentId        -The document Id\
	         * @param {string} attachmentId      -The attachment Id
	         * @returns {string}                 -A conflict link in the format of dbs/{0}/colls/{1}/conflicts/{2} with {0} being a Uri escaped version of the databaseId, {1} being collectionId and {2} being the conflictId
	         * @description Would be used when creating a Conflict in Azure Cosmos DB database service.
	        */
	        createAttachmentUri: function (databaseId, collectionId, documentId, attachmentId) {
	            attachmentId = Helper.trimSlashFromLeftAndRight(attachmentId);
	            Helper.validateResourceId(attachmentId);

	            return this.createDocumentUri(databaseId, collectionId, documentId) + "/" +
	                Constants.Path.AttachmentsPathSegment + "/" +
	                attachmentId;
	        },

	        /**
	         * @summary Given a database and collection, this creates a partition key ranges link in the Azure Cosmos DB database service.
	         * @param {string} databaseId        -The database Id
	         * @param {string} collectionId      -The collection Id
	         * @returns {string}                 -A partition key ranges link in the format of dbs/{0}/colls/{1}/pkranges with {0} being a Uri escaped version of the databaseId and {1} being collectionId
	         */
	        createPartitionKeyRangesUri: function (databaseId, collectionId) {
	            return this.createDocumentCollectionUri(databaseId, collectionId) + "/" +
	                Constants.Path.PartitionKeyRangesPathSegment;
	        }
	    }

	);
	//SCRIPT END

	if (true) {
	    exports.UriFactory = UriFactory;
	}


/***/ }),
/* 142 */
/***/ (function(module, exports, __webpack_require__) {

	var __WEBPACK_AMD_DEFINE_ARRAY__, __WEBPACK_AMD_DEFINE_RESULT__;// async-each MIT license (by Paul Miller from http://paulmillr.com).
	(function(globals) {
	  'use strict';
	  var each = function(items, next, callback) {
	    if (!Array.isArray(items)) throw new TypeError('each() expects array as first argument');
	    if (typeof next !== 'function') throw new TypeError('each() expects function as second argument');
	    if (typeof callback !== 'function') callback = Function.prototype; // no-op

	    if (items.length === 0) return callback(undefined, items);

	    var transformed = new Array(items.length);
	    var count = 0;
	    var returned = false;

	    items.forEach(function(item, index) {
	      next(item, function(error, transformedItem) {
	        if (returned) return;
	        if (error) {
	          returned = true;
	          return callback(error);
	        }
	        transformed[index] = transformedItem;
	        count += 1;
	        if (count === items.length) return callback(undefined, transformed);
	      });
	    });
	  };

	  if (true) {
	    !(__WEBPACK_AMD_DEFINE_ARRAY__ = [], __WEBPACK_AMD_DEFINE_RESULT__ = function() {
	      return each;
	    }.apply(exports, __WEBPACK_AMD_DEFINE_ARRAY__), __WEBPACK_AMD_DEFINE_RESULT__ !== undefined && (module.exports = __WEBPACK_AMD_DEFINE_RESULT__)); // RequireJS
	  } else if (typeof module !== 'undefined' && module.exports) {
	    module.exports = each; // CommonJS
	  } else {
	    globals.asyncEach = each; // <script>
	  }
	})(this);


/***/ })
/******/ ]);