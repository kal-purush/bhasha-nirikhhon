require([
"esri/Map",
"esri/views/MapView",
"esri/widgets/Home",
"esri/layers/TileLayer",
"esri/widgets/Swipe",
"esri/widgets/Expand",
"esri/widgets/BasemapGallery",
"esri/widgets/Locate"
], function(Map, MapView, Home, TileLayer, Swipe, Expand, BasemapGallery, Locate) {
        var mqtLayer = new TileLayer({
        // URL points to a cached tiled map service hosted on ArcGIS Server
        url: "https://tiles.arcgis.com/tiles/b7cJ4YYc9GM63RSz/arcgis/rest/services/marquette1917/MapServer",      
        });

        var ishLayer = new TileLayer({
        // URL points to a cached tiled map service hosted on ArcGIS Server
        url: "https://tiles.arcgis.com/tiles/b7cJ4YYc9GM63RSz/arcgis/rest/services/ispheming1911/MapServer",      
        });

        var negLayer = new TileLayer({
        // URL points to a cached tiled map service hosted on ArcGIS Server
        url: "https://tiles.arcgis.com/tiles/b7cJ4YYc9GM63RSz/arcgis/rest/services/negaunee1911/MapServer",      
        });

        var repLayer = new TileLayer({
        // URL points to a cached tiled map service hosted on ArcGIS Server
        url: "https://tiles.arcgis.com/tiles/b7cJ4YYc9GM63RSz/arcgis/rest/services/republic1922/MapServer",      
        });

        var michLayer = new TileLayer({
        // URL points to a cached tiled map service hosted on ArcGIS Server
        url: "https://tiles.arcgis.com/tiles/b7cJ4YYc9GM63RSz/arcgis/rest/services/michigamme/MapServer",      
        });
        // Add layer to a new map
        var map = new Map({
          basemap: "satellite",
          layers: [ishLayer, mqtLayer, negLayer, repLayer, michLayer]
        });

        var view = new MapView({
          container: "viewDiv",
          map: map,
          center: [-87.3954, 46.5436],
          zoom: 16
        });     

      // set the default opacity of the sanborn layers
      ishLayer.opacity = 100;  
      mqtLayer.opacity = 100;
      negLayer.opacity = 100;
      repLayer.opacity = 100;
      michLayer.opacity = 100;

      // set up the opacity slider 
      var slider = document.getElementById("myRange");         

      // Update the current slider value (each time you drag the slider handle)
      slider.oninput = function() {
        // Change the opacity of the tile layer according to the sliders value
        ishLayer.opacity = this.value / 100;
        mqtLayer.opacity = this.value / 100; 
        negLayer.opacity = this.value / 100;      
        repLayer.opacity = this.value / 100;
        michLayer.opacity = this.value / 100;
      }

      var homeBtn = new Home({
            view: view
        });
        view.ui.add(homeBtn, "top-left");

         // Create a BasemapGallery widget instance and set
      // its container to a div element
      var basemapGallery = new BasemapGallery({
        view: view,
        container: document.createElement("div")
      });

      // Create an Expand instance and set the content
      // property to the DOM node of the basemap gallery widget
      var bgExpand = new Expand({
        view: view,
        content: basemapGallery
      });

      // close the expand whenever a basemap is selected
      basemapGallery.watch("activeBasemap", function() {
        var mobileSize = view.heightBreakpoint === "xsmall" || view.widthBreakpoint === "xsmall";

        if (mobileSize) {
          bgExpand.collapse();
        }
      });

      // Add the expand instance to the ui
      view.ui.add(bgExpand, "top-left");       

       // Add element for the 360 photo viewer button using Esri widgets
  var swipe;     
  var swipeBtn = document.createElement('div');
  swipeBtn.className = "esri-icon-dock-left esri-widget--button esri-widget esri-interactive";
  swipeBtn.title = "Compare maps";
  swipeBtn.addEventListener('click', function(event){
    // create a new Swipe widget
      swipe = new Swipe({
        leadingLayers: [ishLayer, mqtLayer, negLayer, repLayer, michLayer],
        type: "scope",
        //trailingLayers: [nearInfrared],
        position: 50, // set position of widget to 35%
        view: view
      });
    view.ui.add(swipe);
    $(".esri-icon-close").show(); 
    $(".esri-icon-dock-left").hide();
  });

  // Add the button to the UI
  view.ui.add(swipeBtn, "top-left");  

   var closeBtn = document.createElement('div');
  closeBtn.className = "esri-icon-close esri-widget--button esri-widget esri-interactive";
  closeBtn.title = "Exit swipe";
  closeBtn.addEventListener('click', function(event){
    swipe.destroy();
    $(".esri-icon-close").hide(); 
    $(".esri-icon-dock-left").show();
  });

  // Add the button to the UI
  view.ui.add(closeBtn, "top-left");  

  $(".esri-icon-close").hide();

  const locateBtn = new Locate({
          view: view
        });

        // Add the locate widget to the top left corner of the view
        view.ui.add(locateBtn, {
          position: "top-left"
        });

});






/*require([
      "esri/Map",
      "esri/views/MapView",
      "esri/layers/TileLayer",
      "esri/widgets/Swipe",
      "esri/widgets/Expand",
      "esri/widgets/BasemapGallery"
    ], (Map, MapView, TileLayer, Swipe, Expand, BasemapGallery) => {
      // Setup sanborn tile layer
      var sanLayer = new TileLayer({
      // URL points to a cached tiled map service hosted on ArcGIS Server
      url: "https://tiles.arcgis.com/tiles/b7cJ4YYc9GM63RSz/arcgis/rest/services/mqttiles/MapServer/0",      
      });

      // Add layer to a new map
      var map = new Map({
        basemap: "satellite",
        layers: [sanLayer]
      });

      var view = new MapView({
        container: "viewDiv",
        map: map,
        center: [-87.3954, 46.5436],
        zoom: 16
      });

      // set the default opacity of the sanborn layer
      sanLayer.opacity = 100;
      // set up the opacity slider 
      var slider = document.getElementById("myRange");         

      // Update the current slider value (each time you drag the slider handle)
      slider.oninput = function() {
        // Change the opacity of the tile layer according to the sliders value
        sanLayer.opacity = this.value / 100;
        console.log(this.value);
      }

      // Setup ESRI Widgets
      // Create a BasemapGallery widget instance and set
      // its container to a div element
      var basemapGallery = new BasemapGallery({
        view: view,
        container: document.createElement("div")
      });

      // Create an Expand instance and set the content
      // property to the DOM node of the basemap gallery widget
      var bgExpand = new Expand({
        view: view,
        content: basemapGallery
      });

      // close the expand whenever a basemap is selected
      basemapGallery.watch("activeBasemap", function() {
        var mobileSize = view.heightBreakpoint === "xsmall" || view.widthBreakpoint === "xsmall";

        if (mobileSize) {
          bgExpand.collapse();
        }
      });

      // Add the expand instance to the ui
      view.ui.add(bgExpand, "top-left"); 

      // create a new Swipe widget
      var swipe = new Swipe({
        leadingLayers: [sanLayer],
        type: "scope",
        //trailingLayers: [nearInfrared],
        position: 50, // set position of widget to 35%
        view: view
      });
      // add the widget to the view
      view.ui.add(swipe);
    });*/