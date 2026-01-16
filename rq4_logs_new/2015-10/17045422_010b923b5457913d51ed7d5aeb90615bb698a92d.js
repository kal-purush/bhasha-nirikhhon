SQL_BROWSER = (function(sqlbrowser, widgets, aurora, dom, dataManager, binary){
	
	widgets.register("DataBrowser", function(instanceId, data, purgeData) {
		var container = DOM.create("div", undefined, "dataBrowser");
		var sourceSelection = DOM.create("select", undefined, "sql_browser_selector");
		
		var tableContainer = DOM.create("div");
		var objectContainer = DOM.create("div");
		
		container.appendChild(sourceSelection);
		container.appendChild(tableContainer);
		container.appendChild(objectContainer);
		
		return {
		    build : function() {
				return container;
		    },
		    load : function() {
		    	
		    	var sourcesB = DATA.requestB(instanceId, "aurora", aurora.CHANNELS.DATA_SOURCES);
		    	sourcesB.liftB(function(sources){
		    		if(!good()){return sources;}
		    		dom.removeChildren(sourceSelection);
		    		for(var rowIndex in sources.data){
			    		var option = DOM.create("option", undefined, undefined, sources.data[rowIndex].key+" - "+sources.data[rowIndex].description);
			    		option.value = sources.data[rowIndex].key;
			    		sourceSelection.appendChild(option);
		    		}	
		    	});

		    	var dataSourceRowE = F.extractValueE(sourceSelection).mapE(function(selectedKey){
		    		return TABLES.UTIL.findRow(sourcesB.valueNow(), selectedKey);
		    	});
		    	
		    	var dataSourceE = dataSourceRowE.mapE(function(source){	
		    		if(source.type==="sendToClientsE" || source.type==="getChannelE" || source.type==="getCommandChannelE" || source.type==="getTable"){
		    			console.log("PLugin Id:"+aurora.pluginsById[source.pluginId]+" "+source.channelId, source.type);
		    			return DATA.getChannelE(instanceId, aurora.pluginsById[source.pluginId], source.channelId);
		    		}
		    		return F.zeroE();
		    	}).switchE().mapE(function(sourceData){
		    		return binary.arrayBufferToObject(sourceData);
		    	}).mapE(function(data){
		    		objectContainer.style.display = 'block';
					tableContainer.style.display = 'none';
		    		if(typeof(data)==="object"){
		    			objectContainer.innerHTML = JSON.stringify(data);
		    		}
		    		else{
		    			objectContainer.innerHTML = data;
		    		}
		    	});
		    	
				
				var dataSourceB = F.liftB(function(source, state){
					if(state.table){
						state.tableWidget.destroy();
						delete state.tableWidget;
						state.tableWidget = undefined;
						tableBI.purge();
					}
					objectContainer.style.display = 'none';
					tableContainer.style.display = 'none';
					if(source.type==="sendToClientsB" || source.type==="getTable"){
						DOM.removeChildren(tableContainer);
						var tableWidget = new TABLES.WIDGETS.tableWidget(instanceId+"_TW", {});
		    			if(source.type==="sendToClientsB"){
		    				var tableBI = DATA.requestObjectB(instanceId+(new Date().getTime()), aurora.pluginsById[source.pluginId], source.channelId);
		    			}
		    			else if(source.type==="getTable"){
		    				var tableBI = TABLES.sparseTableB(instanceId+(new Date().getTime()), aurora.pluginsById[source.pluginId], source.channelId);
		    			}
		    			tableContainer.appendChild(tableWidget.build());
		    			tableWidget.load(tableBI);
		    			tableContainer.style.display = 'block';
		    			state.tableWidget = tableWidget;
		    			state.tableBI = tableBI;
		    			return tableBI;
		    		}
		    		return F.constantB(SIGNALS.NOT_READY);
				}, dataSourceRowE.startsWith(SIGNALS.NOT_READY), F.constantB({})).switchB().liftB(function(data){
					
					if(good(data) && !TABLES.UTIL.isTable(data)){
						console.log("data: ", data);
						objectContainer.innerHTML = JSON.stringify(data);
						objectContainer.style.display = 'block';
						tableContainer.style.display = 'none';
					}
				});

		    	//dataSourceKeyE
		    	
		    	
		    	//var tableWidget = new TABLES.WIDGETS.tableWidget(instanceId+"_TW", {});
	  		 	//tableWidget.load(tables.sparseTable(instanceId, data.plugin, data.channelId).tableBI);
	  		 	
	  		 	
	  		 	
	  		 //	0: ObjectchannelId: 5description: "Data Sources"key: "aurora_5"pluginId: 1
	  		 	
		    },
		    destroy : function() {
			    
		    }
		};
	});
	
	return sqlbrowser;
}(SQL_BROWSER || {}, WIDGETS, AURORA, DOM, DATA, BINARY));