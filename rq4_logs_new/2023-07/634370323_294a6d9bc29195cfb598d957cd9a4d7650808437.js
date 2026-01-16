
  // Function to handle file selection
  function handleFileSelect(evt) {
    var file = evt.target.files[0];
    var reader = new FileReader();
    reader.onload = function (e) {
      var geojson = JSON.parse(e.target.result);
      var geojsonLayer = L.geoJSON(geojson).addTo(map);
      map.fitBounds(geojsonLayer.getBounds());
    };
    reader.readAsText(file);
  }

  // Add event listener to the import button
  document.getElementById('import-button').addEventListener('click', function () {
    document.getElementById('file-input').click();
  });

  // Add event listener to the file input element
  document.getElementById('file-input').addEventListener('change', handleFileSelect);
