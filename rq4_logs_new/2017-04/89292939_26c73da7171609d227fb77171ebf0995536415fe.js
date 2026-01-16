$(document).ready(function() {
	
	// Alusta kartta
	var mymap = L.map('mapid').setView([60.451, 22.266], 12);
	
	// Luo laattakerros (tile layer) ja attribuutio
	var osmUrl='http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
	var osmAttrib='Map data © <a href="http://openstreetmap.org">OpenStreetMap</a> contributors';
	
	L.tileLayer(osmUrl, { 
		minZoom: 8, 
		maxZoom: 16, 
		attribution: osmAttrib
	}).addTo(mymap);
	
	
	var koordinaatit = [];
	
	var polyline = L.polyline(koordinaatit, {color: 'red'});
	
	var marker = L.marker();
	
	// Alussa poista napit käytöstä
	$('#poistaViimeinen').prop('disabled',true);
	$('#poistaKaikki').prop('disabled',true);
	
	
	//Turku näkyy kartalla painamalla
	$("#Turku").click(function(){
		koordinaatit.push( [60.45093, 22.26639] );
		piirraViiva();
		laskeEtaisyys();
		tulostaKoordinaatit();
		piirraMarker();
		// Otetaan napit käyttöön
		$('#poistaViimeinen').prop('disabled',false);
		$('#poistaKaikki').prop('disabled',false);
})

	//Helsinki 
	$("#Helsinki").click(function() {
	koordinaatit.push( [60.16748, 24.94954] );
		piirraViiva();
		laskeEtaisyys();
		tulostaKoordinaatit();
		piirraMarker();
		// Otetaan napit käyttöön
		$('#poistaViimeinen').prop('disabled',false);
		$('#poistaKaikki').prop('disabled',false);
})
	//Tampere
	$("#Tampere").click(function() {
	koordinaatit.push( [61.48863, 23.75629] );
		piirraViiva();
		laskeEtaisyys();
		tulostaKoordinaatit();
		piirraMarker();
		// Otetaan napit käyttöön
		$('#poistaViimeinen').prop('disabled',false);
		$('#poistaKaikki').prop('disabled',false);
	})
	//espoo
	$("#Espoo").click(function() {
	koordinaatit.push( [60.18796, 24.61267] );
		laskeEtaisyys();
		tulostaKoordinaatit();
		piirraMarker();
		// Otetaan napit käyttöön
		$('#poistaViimeinen').prop('disabled',false);
		$('#poistaKaikki').prop('disabled',false);
})
	
	
	function onMapClick(e) {
		
		koordinaatit.push(e.latlng);
		piirraViiva();
		laskeEtaisyys();
		tulostaKoordinaatit();
		piirraMarker();
		
		// Otetaan napit käyttöön
		$('#poistaViimeinen').prop('disabled',false);
		$('#poistaKaikki').prop('disabled',false);
	}
	
	mymap.on('click', onMapClick);
	
	
	function piirraViiva() {
		polyline.setLatLngs(koordinaatit);
		polyline.addTo(mymap);
	}
	
	function laskeEtaisyys() {
		var pituus = 0;
		if (koordinaatit.length > 1) {
			for (var i = 0; i < koordinaatit.length - 1; i++) {
			pituus = pituus + koordinaatit[i].distanceTo(koordinaatit[i+1]);
			}
		}
		
		$("#etaisyys").val(pituus.toFixed(0));
	}
	
	function tulostaKoordinaatit() {
		// Tyhjennä lista
		$("#koordinaattiLista p").remove();
		
		// Tulosta uusi lista
		var i;
		var arrayLength = koordinaatit.length;
		for (i = 0; i < arrayLength; i++) {
			$("#koordinaattiLista").prepend("<p>" + koordinaatit[i].toString() + "</p>");
		}
	}
	
	function piirraMarker() {
		if (koordinaatit.length === 0) {
		marker.removeFrom(mymap);
		}
		
		if (koordinaatit.length === 1) {
		marker.setLatLng(koordinaatit[0]).addTo(mymap);
		}
	}
	
	
	//Nappi poistaViimeinen
	$('#poistaViimeinen').click(function() {
		koordinaatit.pop();
		piirraViiva();
		laskeEtaisyys();
		tulostaKoordinaatit();
		piirraMarker();
		
		// Poista napit käytöstä, jos ei enää koordinaatteja jäljellä.
		if (koordinaatit.length === 0) {
			$(this).prop('disabled',true);
			$('#poistaKaikki').prop('disabled',true);
		}
	}); //end click
	
	//Nappi poistaKaikki
	$('#poistaKaikki').click(function() {

		var i;
		var arrayLength = koordinaatit.length;
		for (i = 0; i < arrayLength; i++) {
			koordinaatit.pop();
		}
		piirraViiva();
		laskeEtaisyys();
		tulostaKoordinaatit();
		piirraMarker();
		
		// Poista napit käytöstä.
		$(this).prop('disabled',true);
		$('#poistaViimeinen').prop('disabled',true);
	}); //end click
	
	
}); // end ready