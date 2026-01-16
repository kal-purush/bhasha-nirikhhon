/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
var mainurl = 'http://www.kimhauser.ch';

var app = {

    // Application Constructor
    initialize: function() {
        this.bindEvents();
    },
    // Bind Event Listeners
    //
    // Bind any events that are required on startup. Common events are:
    // 'load', 'deviceready', 'offline', and 'online'.
    bindEvents: function() {
        //document.addEventListener('load', this.onLoad, false);
        document.addEventListener('deviceready', this.onDeviceReady, false);
    },
    // deviceready Event Handler
    //
    // The scope of 'this' is the event. In order to call the 'receivedEvent'
    // function, we must explicitly call 'app.receivedEvent(...);'
    //onLoad: function() {
        //app.receivedEvent('load');
    //},
    onDeviceReady: function() {
        //app.receivedEvent('deviceready');
        app.loadLogo();
    },
    onLoadStart: function() {
        window.plugins.spinnerDialog.show("", mainurl, true);
    },
    onLoadStop: function() {
        // This is needed on some phones!
        window.plugins.spinnerDialog.hide();
        window.plugins.spinnerDialog.hide();
    },
    onLoadError: function() {
        alert("Error loading website " + mainurl);
    },
    onExit: function() {
        app.loadWebsite();
    },

    loadLogo: function() {
        theImage = document.getElementById('logo');
        theImage.setAttribute('style', '-webkit-transform:scale(1); opacity:1;');        
        window.setTimeout(function () {
            app.loadWebsite();
        }, 3000);
    },

    loadWebsite: function() {

        // Open in Chrome
        //window.open(mainurl, '_system');

        // Open in WebView
        var onInApp = window.open(mainurl, '_blank', 'location=no,closebuttoncaption=Done,toolbar=no');
        onInApp.addEventListener("loadstart", app.onLoadStart);
        onInApp.addEventListener("loadstop", app.onLoadStop);
        onInApp.addEventListener("loaderror", app.onLoadError);
        onInApp.addEventListener("exit", app.onExit);
    },

    // Update DOM on a Received Event
    /*receivedEvent: function(id) {
        console.log('Received Event: ' + id);
        if(id == "deviceready")
            app.loadLogo();
    }*/
};