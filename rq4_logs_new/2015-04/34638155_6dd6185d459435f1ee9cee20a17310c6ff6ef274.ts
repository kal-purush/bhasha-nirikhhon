// Type definitions for Aerogear 2.1.0
// Project: https://aerogear.org/javascript/
// Definitions by: Ali Ok <http://github.com/aliok>


declare module AeroGear {
    interface DiffSyncClientConfig {
        /**
         * the url of the Differential Sync Server
         */
        serverUrl:string;
        syncEngine?:DiffSyncEngine;
        /**
         * will be called when a connection to the sync server has been opened
         */
        onopen?:(event:Event)=>void;
        /**
         * will be called when a connection to the sync server has been closed
         */
        onclose?:(event:Event)=>void;
        /**
         * listens for "sync" events from the sync server
         */
        onsync?:(doc:SyncDocument, event:Event)=>void;
        /**
         * will be called when there are errors from the sync server
         */
        onerror?:(event:Event)=>void;
    }

    /**
     The AeroGear Differential Sync Client.
     @status Experimental
     */
    class DiffSyncClient {
        /**
         @constructs AeroGear.DiffSyncClient
         @param {DiffSyncClientConfig} config - A configuration
         @returns {DiffSyncClient} diffSyncClient - The created DiffSyncClient
         */
        constructor(config:DiffSyncClientConfig);

        /**
         addDocument - Adds a document to the Sync Engine
         @param {SyncDocument} doc - a document to add to the sync engine
         */
        addDocument(doc:SyncDocument);

        /**
         Disconnects from the Differential Sync Server closing it's Websocket connection
         */
        disconnect();

        /**
         fetch - fetch a document from the Sync Server.  Will perform a sync on it
         @param {string} docId - the id of a document to fetch from the Server
         */
        fetch(docId:string);

        /**
         getDocument - gets the document from the Sync Engine
         @param {string} id - the id of the document to get
         @returns {SyncDocument} - The document from the sync engine
         */
        getDocument(id):SyncDocument;

        /**
         removeDoc
         TODO
         */
        removeDoc(doc:SyncDocument);

        /**
         sync - performs the Sync process
         @param {Object} data - the Data to be sync'd with the server
         */
        sync(data:any);
    }


    /**
     The AeroGear Differential Sync Engine.
     @status Experimental
     */
    class DiffSyncEngine {
        /**
         @constructs AeroGear.DiffSyncEngine
         @param {Object} config - A configuration
         @param {String} [config.type = "jsonPatch"] - the type of sync engine, defaults to jsonPatch
         @returns {DiffSyncEngine} diffSyncEngine - The created DiffSyncEngine
         */
        constructor(config:{type?:string});
    }

    interface SyncDocument {
        id: string;
        clientId: string;
        content:any;
    }
}