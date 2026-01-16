(function() {
	let shadowRoot;
	var Ar = [];
	var ArData = [];
	var ArOptions = [];
	var ArMap = [];
	let template = document.createElement("template");
	
	template.innerHTML = `
		<style>
        body {
            font: 10px arial;
        }
        </style>        
	`;
	
	let firsttime = 0;
	var loader_firsttime = 0;
	
	const loader = "https://www.gstatic.com/charts/loader.js";	
	const gaugejs = "https://ferrygun.github.io/proj/gmap/box/t.js";
	
	function loadScript(src, callback) {
	    const script = document.createElement('script');
	    script.type = 'text/javascript';
	    script.src = src;
	    script.addEventListener("load", callback);
		shadowRoot.appendChild(script);		
	};

	function GoogleChart(divstr, id, value, firsttime) {
		google.setOnLoadCallback(function() {
			drawChart(divstr, id, value, firsttime);
		});
	};

	function drawChart(divstr, id, value, firsttime) {
		var dataTable = new google.visualization.DataTable();
		var newData = JSON.parse("[" + value + "]");
		console.log(newData);
		
		// determine the number of rows and columns.
		var numRows = newData.length;
		var numCols = newData[0].length;
		
		// in this case the first column is of type 'string'.
		dataTable.addColumn('number', newData[0][0]);
		
		// all other columns are of type 'number'.
		for (var i = 1; i < numCols; i++) {
			if (i !== 2) {
				dataTable.addColumn('number', newData[0][i]);
			} else {
				dataTable.addColumn('string', newData[0][i]);
			}
		}
		
		// now add the rows.
		for (var i = 1; i < numRows; i++) {
			dataTable.addRow(newData[i]);
		}
		ArData.push({
			'id': id,
			'data': dataTable
		});
		
		var options = {
			mapType: 'styledMap',
			zoomLevel: 2,
			showTooltip: true,
			showInfoWindow: true,
			useMapTypeControl: true,
			icons: {
			  default: {
				normal: 'https://icons.iconarchive.com/icons/icons-land/vista-map-markers/48/Map-Marker-Ball-Azure-icon.png',
				selected: 'https://icons.iconarchive.com/icons/icons-land/vista-map-markers/48/Map-Marker-Ball-Right-Azure-icon.png'
			  }
			},
			maps: {
				// Your custom mapTypeId holding custom map styles.
				styledMap: {
					name: 'Styled Map', // This name will be displayed in the map type control.
					styles: [{
						featureType: 'poi.attraction',
						stylers: [{
							color: '#fce8b2'
						}]
					}, {
						featureType: 'road.highway',
						stylers: [{
							hue: '#0277bd'
						}, {
							saturation: -50
						}]
					}, {
						featureType: 'road.highway',
						elementType: 'labels.icon',
						stylers: [{
							hue: '#000'
						}, {
							saturation: 100
						}, {
							lightness: 50
						}]
					}, {
						featureType: 'landscape',
						stylers: [{
							hue: '#259b24'
						}, {
							saturation: 10
						}, {
							lightness: -22
						}]
					}]
				}
			}
		};
		
		ArOptions.push({
			'id': id,
			'options': options
		});
		
		var map = new google.visualization.Map(divstr);
		map.draw(dataTable, options);
		
		ArMap.push({
			'id': id,
			'map': map
		});
	};

	function Draw(Ar, firsttime) {
		console.log("** Draw **");
		console.log(Ar);
		for(var i=0; i<Ar.length; i++) {
			GoogleChart(Ar[i].div, Ar[i].id, 0, firsttime);
		}
		
	};
	
	class Box extends HTMLElement {
		constructor() {
			console.log("constructor");
			super();
			
			shadowRoot = this.attachShadow({
				mode: "open"
			});
			
			shadowRoot.appendChild(template.content.cloneNode(true));
			
			this.addEventListener("click", event => {
				console.log('click');
				var event = new Event("onClick");
				this.dispatchEvent(event);
			});
			this._props = {};
		}
		
		connectedCallback() {
			console.log("connectedCallback");
		}
		
		onCustomWidgetBeforeUpdate(changedProperties) {
			console.log("onCustomWidgetBeforeUpdate");
			this._props = {
				...this._props,
				...changedProperties
			};
		}
		
		onCustomWidgetAfterUpdate(changedProperties) {
			console.log("onCustomWidgetAfterUpdate");
			console.log(changedProperties);
		
			if ("value" in changedProperties) {
				console.log("value:" + changedProperties["value"]);
				this.$value = changedProperties["value"];
			}
			
			console.log("firsttime: " + firsttime);
			if (firsttime === 0) {
				const div = document.createElement('div');
				let divid = changedProperties.widgetName;
				div.innerHTML = '<div id="chart_div' + divid + '" style="overflow: hidden; height: 100%; width: 100%; position: absolute;"></div>';
				shadowRoot.appendChild(div);
				
				var mapcanvas_divstr = shadowRoot.getElementById('chart_div' + divid);
				console.log(mapcanvas_divstr);
				Ar.push({
					'id': divid,
					'div': mapcanvas_divstr
				});
				
				var item = this.$value.split(":")[0];
				console.log("item: " + item);
				var data = this.$value.split(":")[1];
				console.log("data: " + data);
				
				/*
				if(loader_firsttime === 0) {
					loadScript(loader, function(){
						 console.log("Load:" + loader);
					});
					loader_firsttime = 1;
				}
				*/
				
				loadScript(loader, function(){
					console.log("Load:" + loader);
					
					loadScript(gaugejs, function(){
						console.log("Load:" + gaugejs);

						var data = '["Lat","Long","Name"]';
						Draw(mapcanvas_divstr, divid, data, firsttime);
						firsttime = 1;
					});
				});
				
			} else {
				var item = this.$value.split(":")[0];
				console.log("item: " + item);
				var data = this.$value.split(":")[1];
				console.log("data: " + data);
				
				if (data !== "") {
					var foundIndex = Ar.findIndex(x => x.id == item);
					console.log("foundIndex: " + foundIndex);
					if (foundIndex !== -1) {
						console.log(Ar[foundIndex].div);
						drawChart(Ar[foundIndex].div, Ar[foundIndex].id, data, firsttime);
					}
				}
			}
		}
	}
	customElements.define("com-fd-gmap", Box);
})();