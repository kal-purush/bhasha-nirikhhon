var fs = require('fs');

sap.ui.define([
	"sap/ui/core/UIComponent",
	"sap/ui/model/json/JSONModel"
], function (UIComponent, JSONModel ) {
	"use strict";

	return UIComponent.extend("app.Component", {

		metadata: {
			manifest: "json",
            events:{
            }
		},
		init: function () {
			// call the init function of the parent
			UIComponent.prototype.init.apply(this, arguments);
			this.oFiles = new JSONModel([]);
			this.setModel(this.oFiles,"Files");
			this.paths=[];
			this.readFolder();
			this.oFile = new JSONModel({content:""});
			this.setModel(this.oFile,"File");
        },
		drilldown:function(sName){
			this.paths.push(sName);
			this.readFolder();
		},
		readFolder:function(){
			fs.readdir(__dirname + "/" + this.paths.join("/"),function(err,files){
				this.oFiles.setData(files);
			}.bind(this));
		},
		back:function(){
			this.paths.pop();
			this.readFolder();
		},
		readFile:function(sName){
			fs.readFile(__dirname + "/" + this.paths.join("/") + "/" + sName, 'utf8',function(err,content){
				this.oFile.setProperty("/content",content);
			}.bind(this));
		}
	});

});