alert("raritan works");

google.maps.event.addDomListener(window, 'load', initialize);

function initialize() {

	alert("raritan works");
	
	//Polygon contains lat/lng function

	// Polygon getBounds extension - google-maps-extensions
	// https://github.com/tparkin/Google-Maps-Point-in-Polygon
	// http://code.google.com/p/google-maps-extensions/source/browse/google.maps.Polygon.getBounds.js
	if (!google.maps.Polygon.prototype.getBounds) {
	  google.maps.Polygon.prototype.getBounds = function(latLng) {
	    var bounds = new google.maps.LatLngBounds(),
	      paths = this.getPaths(),
	      path,
	      p, i;

	    for (p = 0; p < paths.getLength(); p++) {
	      path = paths.getAt(p);
	      for (i = 0; i < path.getLength(); i++) {
	        bounds.extend(path.getAt(i));
	      }
	    }

	    return bounds;
	  };
	}

	// Polygon containsLatLng - method to determine if a latLng is within a polygon
	google.maps.Polygon.prototype.containsLatLng = function(latLng) {
	  // Exclude points outside of bounds as there is no way they are in the poly


	  var inPoly = false,
	    bounds, lat, lng,
	    numPaths, p, path, numPoints,
	    i, j, vertex1, vertex2;

	  // Arguments are a pair of lat, lng variables
	  if (arguments.length == 3) {
	    if (
	      typeof arguments[0] == "number" &&
	      typeof arguments[1] == "number" &&
	      typeof arguments[2] == "function"
	    ) {
	      lat = arguments[0];
	      lng = arguments[1];
	      //callback = arguments[2];
	    }
	  } else if (arguments.length == 1) {
	    bounds = this.getBounds();

	    if (!bounds && !bounds.contains(latLng)) {
	      return false;
	    }
	    lat = latLng.lat();
	    lng = latLng.lng();
	  } else {
	    console.log("Wrong number of inputs in google.maps.Polygon.prototype.contains.LatLng");
	  }

	  // Raycast point in polygon method

	  numPaths = this.getPaths().getLength();
	  for (p = 0; p < numPaths; p++) {
	    path = this.getPaths().getAt(p);
	    numPoints = path.getLength();
	    j = numPoints - 1;

	    for (i = 0; i < numPoints; i++) {
	      vertex1 = path.getAt(i);
	      vertex2 = path.getAt(j);

	      if (
	        vertex1.lng() <  lng &&
	        vertex2.lng() >= lng ||
	        vertex2.lng() <  lng &&
	        vertex1.lng() >= lng
	      ) {
	        if (
	          vertex1.lat() +
	          (lng - vertex1.lng()) /
	          (vertex2.lng() - vertex1.lng()) *
	          (vertex2.lat() - vertex1.lat()) <
	          lat
	        ) {
	          inPoly = !inPoly;
	        }
	      }

	      j = i;
	    }
	  }
	  //callback(inPoly);
	  return inPoly;
	};


	// Fish warning area polygons

		// Arthur Kill, Newark Bay and Kill Van Kull
		var arthurkill;
		var arthurkillCoords = [
		    new google.maps.LatLng(40.502016, -74.277058),
		    new google.maps.LatLng(40.498981, -74.269591),
		    new google.maps.LatLng(40.496044, -74.248476),
		    new google.maps.LatLng(40.504368, -74.245801),
			new google.maps.LatLng(40.511903, -74.245386),
			new google.maps.LatLng(40.518885, -74.235859),
			new google.maps.LatLng(40.531346, -74.233370),
			new google.maps.LatLng(40.542044, -74.235087),
			new google.maps.LatLng(40.551240, -74.211655),
			new google.maps.LatLng(40.562456, -74.199639),
			new google.maps.LatLng(40.568585, -74.183417),
			new google.maps.LatLng(40.559652, -74.165221),
			new google.maps.LatLng(40.564738, -74.152947),
			new google.maps.LatLng(40.573540, -74.143076),
			new google.maps.LatLng(40.580611, -74.157204),
		    new google.maps.LatLng(40.595733, -74.163985),
		    new google.maps.LatLng(40.606095, -74.171881),
			new google.maps.LatLng(40.625641, -74.165701), //Arthur kills east point
			new google.maps.LatLng(40.631634, -74.161152),
		 	new google.maps.LatLng(40.642446, -74.089612), //south east point
			new google.maps.LatLng(40.656056, -74.093904), //north east point
			new google.maps.LatLng(40.654884, -74.117335),
			new google.maps.LatLng(40.653126, -74.121198),
			new google.maps.LatLng(40.649479, -74.123515),
			new google.maps.LatLng(40.649154, -74.138106),
			new google.maps.LatLng(40.651758, -74.177460), // Arthur Kills north point
		    new google.maps.LatLng(40.648274, -74.194626),
		    new google.maps.LatLng(40.633685, -74.210419),
			new google.maps.LatLng(40.625738, -74.212651),
		    new google.maps.LatLng(40.612057, -74.218315),
		    new google.maps.LatLng(40.588596, -74.221749),
			new google.maps.LatLng(40.572430, -74.227070),
		    new google.maps.LatLng(40.557302, -74.261574),
		    new google.maps.LatLng(40.549346, -74.275650),
			new google.maps.LatLng(40.516729, -74.277882),
		    new google.maps.LatLng(40.502016, -74.277058)
		];

		arthurkill = new google.maps.Polygon({
			paths: arthurkillCoords,
			strokeColor: '#FF0000',
			strokeOpacity: 0.5,
			strokeWeight: 2,
			fillColor: '#FF0000',
			fillOpacity: 0.5,
			visible: true
		});

		arthurkill.setMap(map);


		// East River to the Throgs Neck, Harlem, Hudson and Bronx Rivers)
		var eastriver;
		var eastriverCoords = [
			new google.maps.LatLng(40.791102, -73.792677),
			new google.maps.LatLng(40.788893, -73.791303),
			new google.maps.LatLng(40.787528, -73.789673),
			new google.maps.LatLng(40.782459, -73.785209),
			new google.maps.LatLng(40.783499, -73.795166),
			new google.maps.LatLng(40.788178, -73.799715),
			new google.maps.LatLng(40.792402, -73.821258),
			new google.maps.LatLng(40.783889, -73.828297),
			new google.maps.LatLng(40.783824, -73.840914),
			new google.maps.LatLng(40.773945, -73.839884),
			new google.maps.LatLng(40.764324, -73.829584),
			new google.maps.LatLng(40.746900, -73.829241),
			new google.maps.LatLng(40.744364, -73.850355),
			new google.maps.LatLng(40.753857, -73.857393),
			new google.maps.LatLng(40.762309, -73.869066),
			new google.maps.LatLng(40.767250, -73.892069),
			new google.maps.LatLng(40.776090, -73.907004),
			new google.maps.LatLng(40.763479, -73.932924),
			new google.maps.LatLng(40.750346, -73.944941),
		 	new google.maps.LatLng(40.739356, -73.930006),
			new google.maps.LatLng(40.728820, -73.917475),
			new google.maps.LatLng(40.722706, -73.912840),
			new google.maps.LatLng(40.713729, -73.914900),
			new google.maps.LatLng(40.703709, -73.927775),
			new google.maps.LatLng(40.705661, -73.937045),
			new google.maps.LatLng(40.716191, -73.936733),
			new google.maps.LatLng(40.720875, -73.930811),
			new google.maps.LatLng(40.725298, -73.935960),
			new google.maps.LatLng(40.728092, -73.941570),
			new google.maps.LatLng(40.731084, -73.947234),
		 	new google.maps.LatLng(40.733685, -73.954616),
			new google.maps.LatLng(40.724709, -73.953758),
			new google.maps.LatLng(40.715602, -73.960796),
			new google.maps.LatLng(40.706233, -73.962856),
			new google.maps.LatLng(40.696213, -73.964572),
			new google.maps.LatLng(40.696799, -73.980022),
			new google.maps.LatLng(40.697710, -73.991008),
			new google.maps.LatLng(40.679519, -74.004054),
			new google.maps.LatLng(40.694197, -74.013023),
			new google.maps.LatLng(40.702429, -74.015856), //bottom intersection (east river)
			new google.maps.LatLng(40.706756, -74.012852), //top intersection (east river)
			new google.maps.LatLng(40.711993, -74.003754),
		 	new google.maps.LatLng(40.714075, -73.982296),
			new google.maps.LatLng(40.721361, -73.979721),
			new google.maps.LatLng(40.727996, -73.976974),
			new google.maps.LatLng(40.734175, -73.978348),
			new google.maps.LatLng(40.738597, -73.978262),
			new google.maps.LatLng(40.744905, -73.974915),
			new google.maps.LatLng(40.755569, -73.968048),
			new google.maps.LatLng(40.763110, -73.960667),
			new google.maps.LatLng(40.770001, -73.953629),
			new google.maps.LatLng(40.777541, -73.948822),
			new google.maps.LatLng(40.784040, -73.949509),
			new google.maps.LatLng(40.794762, -73.940926),
		 	new google.maps.LatLng(40.799051, -73.935604), 
			new google.maps.LatLng(40.805808, -73.937836), 
			new google.maps.LatLng(40.812694, -73.938694),
			new google.maps.LatLng(40.833219, -73.939037),
			new google.maps.LatLng(40.839193, -73.937321),
			new google.maps.LatLng(40.856983, -73.926506),
			new google.maps.LatLng(40.869965, -73.917580),
			new google.maps.LatLng(40.870809, -73.921465), 
			new google.maps.LatLng(40.864161, -73.924921),
			new google.maps.LatLng(40.838971, -73.942087),
			new google.maps.LatLng(40.827931, -73.947924),
			new google.maps.LatLng(40.800518, -73.968523),
			new google.maps.LatLng(40.770104, -73.990667),
			new google.maps.LatLng(40.750600, -74.003971),
			new google.maps.LatLng(40.737985, -74.005516),
			new google.maps.LatLng(40.713658, -74.010151),
			new google.maps.LatLng(40.706756, -74.012852), //top intersection (harlem)
			new google.maps.LatLng(40.702429, -74.015856), //bottom intersection (harlem)
			new google.maps.LatLng(40.705981, -74.048260),
		 	new google.maps.LatLng(40.710991, -74.057701),
			new google.maps.LatLng(40.715935, -74.054096),
			new google.maps.LatLng(40.718017, -74.039505),
			new google.maps.LatLng(40.734279, -74.036587),
			new google.maps.LatLng(40.739351, -74.031952),
			new google.maps.LatLng(40.746374, -74.028004),
			new google.maps.LatLng(40.752227, -74.028175),
			new google.maps.LatLng(40.755868, -74.030922),
			new google.maps.LatLng(40.760159, -74.028862),
			new google.maps.LatLng(40.761069, -74.024914),
			new google.maps.LatLng(40.771600, -74.017533),
		 	new google.maps.LatLng(40.774460, -74.014271),
			new google.maps.LatLng(40.780180, -74.011009),
			new google.maps.LatLng(40.801494, -73.996761),
			new google.maps.LatLng(40.814877, -73.986118),
			new google.maps.LatLng(40.833453, -73.975647),
			new google.maps.LatLng(40.840726, -73.974273),
			new google.maps.LatLng(40.852153, -73.966034),
			new google.maps.LatLng(40.862410, -73.962944),
			new google.maps.LatLng(40.882919, -73.952472),
			new google.maps.LatLng(40.920997, -73.936637),
			new google.maps.LatLng(40.919441, -73.905008), 
		 	new google.maps.LatLng(40.896867, -73.908098),
			new google.maps.LatLng(40.881258, -73.915039), 
			new google.maps.LatLng(40.875677, -73.903503),
			new google.maps.LatLng(40.866330, -73.904190),
			new google.maps.LatLng(40.854256, -73.914318),
			new google.maps.LatLng(40.840752, -73.924274),
			new google.maps.LatLng(40.833219, -73.927707),
			new google.maps.LatLng(40.811266, -73.926506),
			new google.maps.LatLng(40.805679, -73.920669),
			new google.maps.LatLng(40.801780, -73.913631),
			new google.maps.LatLng(40.807628, -73.907108),
			new google.maps.LatLng(40.810746, -73.893375),
			new google.maps.LatLng(40.807888, -73.883419),
			new google.maps.LatLng(40.808667, -73.877410),
			new google.maps.LatLng(40.817372, -73.888397),
			new google.maps.LatLng(40.831206, -73.888397),
		 	new google.maps.LatLng(40.845492, -73.880672),
			new google.maps.LatLng(40.868603, -73.882217),
			new google.maps.LatLng(40.881972, -73.873291),
			new google.maps.LatLng(40.879766, -73.864536),
			new google.maps.LatLng(40.859256, -73.869343),
			new google.maps.LatLng(40.848349, -73.867798),
			new google.maps.LatLng(40.832895, -73.875007),
			new google.maps.LatLng(40.825361, -73.878612),
			new google.maps.LatLng(40.820295, -73.876209),
			new google.maps.LatLng(40.816527, -73.862991),
			new google.maps.LatLng(40.811071, -73.853378),
			new google.maps.LatLng(40.813279, -73.837757),
		 	new google.maps.LatLng(40.817437, -73.825054),
			new google.maps.LatLng(40.816008, -73.810119), //throgs neck
			new google.maps.LatLng(40.810811, -73.804969),
			new google.maps.LatLng(40.809122, -73.800678),
			new google.maps.LatLng(40.805744, -73.793296),
			new google.maps.LatLng(40.799094, -73.793363) //middle of throgs
		];

		eastriver = new google.maps.Polygon({
			paths: eastriverCoords,
			strokeColor: '#FF0000',
			strokeOpacity: 0,
			strokeWeight: 2,
			fillColor: '#FF0000',
			fillOpacity: 0,
			visible: false
		});

		eastriver.setMap(map);


		// Paerdegat Basin
		var paerdegat;
		var paerdegatCoords = [
			new google.maps.LatLng(40.619898, -73.898672), //center of maguire
		 	new google.maps.LatLng(40.618986, -73.896268),
			new google.maps.LatLng(40.618725, -73.893693),
			new google.maps.LatLng(40.620158, -73.891719),
			new google.maps.LatLng(40.622895, -73.891891),
			new google.maps.LatLng(40.624784, -73.894294), //endof mouth on right
			new google.maps.LatLng(40.634490, -73.916610),
			new google.maps.LatLng(40.633252, -73.919700),
			new google.maps.LatLng(40.629995, -73.920644),
			new google.maps.LatLng(40.622504, -73.905194),
			new google.maps.LatLng(40.619963, -73.901933),
			new google.maps.LatLng(40.619898, -73.898672)
		];

		paerdegat = new google.maps.Polygon({
			paths: paerdegatCoords,
			strokeColor: '#FF0000',
			strokeOpacity: 0,
			strokeWeight: 2,
			fillColor: '#FF0000',
			fillOpacity: 0,
			visible: false
		});

		paerdegat.setMap(map);


		// Jamaica Bay, Atlantic Ocean/Long Island South Shore, Lower NY Bay, Raritan Bay East of Wolf's Pond Park
		var jamaica;
		var jamaicaCoords = [
			new google.maps.LatLng(40.595687, -73.712042),
			new google.maps.LatLng(40.433079, -73.986013),
			new google.maps.LatLng(40.444837, -73.987043), 
			new google.maps.LatLng(40.472788, -74.013136), //southern lower bay entrance/east sandy hook bay
			new google.maps.LatLng(40.451093, -74.135766), //western sandy hook entrance
		 	new google.maps.LatLng(40.437114, -74.156537),
			new google.maps.LatLng(40.449003, -74.178853),
			new google.maps.LatLng(40.530076, -74.181600),
			new google.maps.LatLng(40.542861, -74.156194),
			new google.maps.LatLng(40.568425, -74.125638),
			new google.maps.LatLng(40.598149, -74.072080),
			new google.maps.LatLng(40.601016, -74.063154), //west upper bay mouth
			new google.maps.LatLng(40.614570, -74.022126), //east upper bay mouth
			new google.maps.LatLng(40.592674, -73.977837),
			new google.maps.LatLng(40.600234, -73.941102),
			new google.maps.LatLng(40.614309, -73.941102),
			new google.maps.LatLng(40.619898, -73.898672), //center of maguire
		 	new google.maps.LatLng(40.618986, -73.896268),
			new google.maps.LatLng(40.618725, -73.893693),
			new google.maps.LatLng(40.620158, -73.891719),
			new google.maps.LatLng(40.622895, -73.891891),
			new google.maps.LatLng(40.624784, -73.894294),
			new google.maps.LatLng(40.633252, -73.891547),
			new google.maps.LatLng(40.638463, -73.889316),
		 	new google.maps.LatLng(40.650707, -73.898586),
			new google.maps.LatLng(40.667375, -73.859275),
			new google.maps.LatLng(40.669979, -73.822196),
			new google.maps.LatLng(40.669198, -73.766235),
			new google.maps.LatLng(40.670239, -73.748382),
			new google.maps.LatLng(40.666594, -73.727439),
			new google.maps.LatLng(40.666529, -73.702119),
			new google.maps.LatLng(40.658781, -73.690188),
			new google.maps.LatLng(40.643478, -73.699029),
			new google.maps.LatLng(40.630712, -73.722203),
			new google.maps.LatLng(40.625696, -73.733447),
			new google.maps.LatLng(40.615533, -73.734563)
		];

		jamaica = new google.maps.Polygon({
			paths: jamaicaCoords,
			strokeColor: '#FF0000',
			strokeOpacity: 0,
			strokeWeight: 2,
			fillColor: '#FF0000',
			fillOpacity: 0,
			visible: false
		});

		jamaica.setMap(map);


		// Long Island Sound
		var lisound;
		var lisoundCoords = [
			new google.maps.LatLng(40.816008, -73.810119), //throgs neck top
			new google.maps.LatLng(40.810811, -73.804969),
			new google.maps.LatLng(40.809122, -73.800678),
			new google.maps.LatLng(40.805744, -73.793296),
			new google.maps.LatLng(40.799094, -73.793363), //middle of throgs
			new google.maps.LatLng(40.791102, -73.792677),
			new google.maps.LatLng(40.788893, -73.791303),
			new google.maps.LatLng(40.787528, -73.789673),
			new google.maps.LatLng(40.782459, -73.785209),
			new google.maps.LatLng(40.761659, -73.759460),
		 	new google.maps.LatLng(40.755678, -73.750190),
			new google.maps.LatLng(40.755678, -73.750190),
			new google.maps.LatLng(40.788958, -73.665046),
			new google.maps.LatLng(40.844821, -73.690109),
			new google.maps.LatLng(40.862220, -73.723411),
			new google.maps.LatLng(40.914908, -73.822288),
			new google.maps.LatLng(40.893889, -73.839111),
			new google.maps.LatLng(40.855988, -73.841514),
			new google.maps.LatLng(40.823023, -73.818874)
		];

		lisound = new google.maps.Polygon({
			paths: lisoundCoords,
			strokeColor: '#FF0000',
			strokeOpacity: 0,
			strokeWeight: 2,
			fillColor: '#FF0000',
			fillOpacity: 0,
			visible: false
		});

		lisound.setMap(map);


		// Upper Bay (hudson, east, lower bay, kill van kull)
		var upper;
		var upperCoords = [
			new google.maps.LatLng(40.679519, -74.004054), //carrol gardens
			new google.maps.LatLng(40.694197, -74.013023),
			new google.maps.LatLng(40.702429, -74.015856), //bottom intersection (east river)
			new google.maps.LatLng(40.705981, -74.048260),
		 	new google.maps.LatLng(40.710991, -74.057701), //liberty science center
			new google.maps.LatLng(40.701622, -74.076069),
		 	new google.maps.LatLng(40.688086, -74.093578),
			new google.maps.LatLng(40.669860, -74.113319),
			new google.maps.LatLng(40.656056, -74.093904), //kill van kull north
			new google.maps.LatLng(40.642446, -74.089612), //kill van kull south
			new google.maps.LatLng(40.641376, -74.081232), 
			new google.maps.LatLng(40.626785, -74.080717),
			new google.maps.LatLng(40.605415, -74.064753),
			new google.maps.LatLng(40.601016, -74.063154), //west upper bay mouth
			new google.maps.LatLng(40.614570, -74.022126), //east upper bay mouth
			new google.maps.LatLng(40.633592, -74.028649),
			new google.maps.LatLng(40.653000, -74.005818),
			new google.maps.LatLng(40.661725, -73.988652),
			new google.maps.LatLng(40.683077, -73.979211),
			new google.maps.LatLng(40.687503, -73.989167),
			new google.maps.LatLng(40.679519, -74.004054)
		];

		upper = new google.maps.Polygon({
			paths: upperCoords,
			strokeColor: '#FF0000',
			strokeOpacity: 0,
			strokeWeight: 2,
			fillColor: '#FF0000',
			fillOpacity: 0,
			visible: false
		});

		upper.setMap(map);


		// Raritan Bay West of Wolfe's Pond Park
		var raritan;
		var raritanCoords = [
			new google.maps.LatLng(40.504368, -74.245801), //Tottenville
		    new google.maps.LatLng(40.496044, -74.248476),
		    new google.maps.LatLng(40.498981, -74.269591),
		    new google.maps.LatLng(40.502016, -74.277058),
		 	new google.maps.LatLng(40.488048, -74.286156),
			new google.maps.LatLng(40.465066, -74.272423),
			new google.maps.LatLng(40.437765, -74.296799),
			new google.maps.LatLng(40.425483, -74.277058),
			new google.maps.LatLng(40.429926, -74.250794),
			new google.maps.LatLng(40.414113, -74.229508),
			new google.maps.LatLng(40.430971, -74.201527),
			new google.maps.LatLng(40.449003, -74.178853), //south east bound
			new google.maps.LatLng(40.530076, -74.181600), //north east bound
			new google.maps.LatLng(40.525314, -74.210010),
			new google.maps.LatLng(40.504368, -74.245801)
		];

		raritan = new google.maps.Polygon({
			paths: raritanCoords,
			strokeColor: '#FF0000',
			strokeOpacity: 0.5,
			strokeWeight: 2,
			fillColor: '#FF0000',
			fillOpacity: 0.5,
			//visible: true
		});

		raritan.setMap(map);

		function getQueryVariable(variable)
		{
		       var query = window.location.search.substring(1);
		       var vars = query.split("&");
		       for (var i=0;i<vars.length;i++) {
		               var pair = vars[i].split("=");
		               if(pair[0] == variable){return pair[1];}
		       }
		       return(false);
		}

		var alert_isWithin = function(isWithin_site){
			alert(isWithin_site);
		}

		var fish_warning_lat = parseFloat(getQueryVariable("lat"));
		var fish_warning_lng = parseFloat(getQueryVariable("lng"));

		alert("alert works");


		var isWithin_arthurkill = arthurkill.containsLatLng(fish_warning_lat, fish_warning_lng, alert_isWithin);
		var isWithin_eastriver = eastriver.containsLatLng(fish_warning_lat, fish_warning_lng, alert_isWithin(eastriver));
		var isWithin_paerdegat = paerdegat.containsLatLng(fish_warning_lat, fish_warning_lng, alert_isWithin(paerdegat));
		var isWithin_jamaica = jamaica.containsLatLng(fish_warning_lat, fish_warning_lng, alert_isWithin(jamaica));
		var isWithin_lisound = lisound.containsLatLng(fish_warning_lat, fish_warning_lng, alert_isWithin(lisound));
		var isWithin_upper = upper.containsLatLng(fish_warning_lat, fish_warning_lng, alert_isWithin(upper));
		var isWithin_raritan = raritan.containsLatLng(fish_warning_lat, fish_warning_lng, alert_isWithin(raritan));
	

		//alert(isWithin_arthurkill);
		//alert(isWithin_eastriver);
		//alert(isWithin_paerdegat);
		//alert(isWithin_jamaica);
		//alert(isWithin_lisound);
		//alert(isWithin_upper);
		//alert(isWithin_raritan);



		if (isWithin_arthurkill == true) {
			alert("arthurkill")
		}  




		// var isWithinPolygon = polygon.containsLatLng(lat, lng);


		// set isWithinPolygon for each site
		// grab params lat long and check if isWithinPolygon is true/false
		// if isWithinPolygon is true, alert(fish is in Polygon)




	}
