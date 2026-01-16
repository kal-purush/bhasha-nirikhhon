function insertAfter(newNode, referenceNode) {
    referenceNode.parentNode.insertBefore(newNode, referenceNode.nextSibling);
}

function toggleVisibility(layer) {
    layer.show = layer.show ? false : true;
}

//<button class="cesium-button cesium-toolbar-button" id="layersbutton" title="Показать исторические карты"><img src="sourcedata/layers_icon.png"></button>
var layersButton = document.createElement("button");
layersButton.classList.add("cesium-button", "cesium-toolbar-button");
layersButton.id = "layersbutton";
layersButton.title = "Показать исторические карты";
layersButton.onclick = function() {toggleVisibility(imageryLayer);}
var layersIcon = document.createElement("img");
layersIcon.src = "sourcedata/layers_icon.png";
layersButton.appendChild(layersIcon);
document.getElementById("cesiumContainer").appendChild(layersButton);
//var layersButton = document.getElementById("layersbutton");
var toolbar = document.getElementsByClassName("cesium-viewer-toolbar")[0];
var toolbarHome = document.getElementsByClassName("cesium-home-button")[0];

toolbar.insertAfter = insertAfter;
toolbar.insertAfter(layersButton,toolbarHome);