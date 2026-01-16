QUnit.module('Map Tests', {
	beforeEach: function(assert){
		this.fixture = $( '#qunit-fixture' );
		this.fixture.append('<div id="map"></div>');
		this.map = $.mapModule({exposedForTesting: true});
		this.map.init();
		assert.ok(this.map, 'The map exists.');
		assert.ok(this.map.mapObject, 'The leaflet map object exists.');
		assert.equal(this.map.currentOffice, -1, 'The current office is not yet set.');
		assert.deepEqual(this.map.officeList, [], 'The office list is empty at start-up.');
		assert.deepEqual(this.map.activeLayers, [], 'Active layers list is empty at start-up');
		assert.deepEqual(this.map.levelControl, {}, 'No level controls have been added yet.');
	}
});

QUnit.test('Testing map initialization.', function(assert){
	assert.expect(6);
});

QUnit.test('Adding offices.', function(assert){
	this.fixture.append('<ul class="dropdown-menu" id="officeList"></ul>');
	var officeDropdown = $('#officeList');
	
	// office 1
	this.map.addOffice(1, 'Conshohocken');
	assert.equal(this.map.officeList.length, 1, 'Office list has 1 element.');
	assert.deepEqual(this.map.officeList, [{'id': 1, 'name': 'Conshohocken', 'floorList': []}], 'Office 1 added.');
	assert.equal(officeDropdown.children().length, 1, 'Office dropdown contains 1 child.');
	assert.deepEqual(officeDropdown.children()[0].innerHTML, '<a href="#" id="office1">Conshohocken</a>', 'Dropdown contains office 1');
	
	// office 2
	this.map.addOffice(2, 'San Diego');
	assert.equal(this.map.officeList.length, 2, 'Office list has 2 elements.');
	assert.deepEqual(this.map.officeList, [
			{'id': 1, 'name': 'Conshohocken', 'floorList': []},
			{'id': 2, 'name': 'San Diego', 'floorList': []},
		], 'Office 2 added.');
	assert.equal(officeDropdown.children().length, 2, 'Office dropdown contains 2 children.');
	assert.deepEqual(officeDropdown.children()[0].innerHTML, '<a href="#" id="office1">Conshohocken</a>', 'Dropdown contains office 1');
	assert.deepEqual(officeDropdown.children()[1].innerHTML, '<a href="#" id="office2">San Diego</a>', 'Dropdown contains office 2');

	// floors
	this.map.addFloor(1, 1, 'testImage.png', [[49.41873, 8.67689], [49.41973, 8.67959]]);
	this.map.addFloor(1, 2, 'testImage.png', [[49.41873, 8.67689], [49.41973, 8.67959]]);
	this.map.addFloor(2, 1, 'testImage.png', [[49.41873, 8.67689], [49.41973, 8.67959]]);
	
	assert.equal(this.map.getOffice(1).floorList.length, 2, 'Office 1 has 2 floors.');
	assert.equal(this.map.getOffice(2).floorList.length, 1, 'Office 2 has 1 floor.');
	// office 1 floor 1
	assert.equal(this.map.getOffice(1).floorList[0].floor, 1, 'Office 1 floor 1 has correct floor number.');
	assert.equal(this.map.getOffice(1).floorList[0].url, 'testImage.png', 'Office 1 floor 1 has correct image URL.');
	assert.ok(this.map.getOffice(1).floorList[0].image, 'Office 1 floor 1 has an image object');
	assert.deepEqual(this.map.getOffice(1).floorList[0].markers, [], 'Office 1 floor 1 has an empty markers list.');
	// office 1 floor 2
	assert.equal(this.map.getOffice(1).floorList[1].floor, 2, 'Office 1 floor 2 has correct floor number.');
	assert.equal(this.map.getOffice(1).floorList[1].url, 'testImage.png', 'Office 1 floor 2 has correct image URL.');
	assert.ok(this.map.getOffice(1).floorList[1].image, 'Office 1 floor 2 has an image object');
	assert.deepEqual(this.map.getOffice(1).floorList[1].markers, [], 'Office 1 floor 2 has an empty markers list.');
	// office 2 floor 1
	assert.equal(this.map.getOffice(2).floorList[0].floor, 1, 'Office 2 floor 1 has correct floor number.');
	assert.equal(this.map.getOffice(2).floorList[0].url, 'testImage.png', 'Office 2 floor 1 has correct image URL.');
	assert.ok(this.map.getOffice(2).floorList[0].image, 'Office 2 floor 1 has an image object');
	assert.deepEqual(this.map.getOffice(2).floorList[0].markers, [], 'Office 2 floor 1 has an empty markers list.');	
});

QUnit.module('Data Tests');

QUnit.test('Getting map data.', function(assert){
	var done = assert.async();
	var controller = $.mapController();
	controller.getData('../tests/data.json', 'json', function(data){
		var testData = data;
		assert.ok(testData, 'Data exists.');
		assert.equal(testData.type, 'FeatureCollection', 'Data has the correct type');
		assert.equal(testData.features.length, 3, 'Data has the correct number of features');
		var feature = {
			"type":"Feature",
			"properties":{
				"switchName": "emaswitch00",
				"port": "A1",
				"office":1,
				"level":1,
				"popupContent":""
			},
			"users":[],
			"geometry":{  
				"type":"Point",
				"coordinates":[  
					8.67746,
					49.41931
				]
			}
      };
	  
	  assert.deepEqual(testData.features[0], feature, 'First feature matches.');
	  
	  feature = {
		"type":"Feature",
		"properties":{
			"switchName": "emaswitch00",
			"port": "A2",
			"office":1,
			"level":1,
			"popupContent":""
		},
		"users":[],
		"geometry":{
			"type":"Point",
			"coordinates":[  
				8.67754,
				49.41931
			]
		}
		}
	  
	  assert.deepEqual(testData.features[1], feature, 'Second feature matches.');
	  
		feature = {  
			"type":"Feature",
			"properties":{
				"switchName": "emaswitch01",
				"port": "B1",
				"office":2,
				"level":1,
				"popupContent":""
			},
			"users":[],
			"geometry":{
				"type":"Point",
				"coordinates":[  
					8.67746,
					49.41931
				]
			}
		}
	  
	  assert.deepEqual(testData.features[2], feature, 'Third feature matches.');
	  done();
	});
});

QUnit.test('Getting user data', function(assert){
	var done = assert.async();
	var controller = $.mapController();
	controller.getData('../tests/userData.json', 'json', function(data){
		var testData = data;
		assert.ok(testData, 'Data exists.');
		assert.ok(testData.emaswitch00, 'emaswitch00 found');
		assert.ok(testData.emaswitch01, 'emaswitch01 found');
		assert.ok(testData.emaswitch00.ports.A1, 'emaswitch00 port A1 found');
		var A1 = {
            "id": 1,
			"firstName":"John",
			"lastName":"Doe",
			"userName":"jdoe",
			"city":"Newark",
			"department":"IT",
			"email":"jdoe@email.com"
		};
		assert.deepEqual(testData.emaswitch00.ports.A1, A1, 'emaswitch00 port A1 matches');
		
		assert.ok(testData.emaswitch00.ports.A2, 'emaswitch00 port A2 found');
		var A2 = {
            "id": 2,
			"firstName":"Jane",
			"lastName":"Doe",
			"userName":"doej",
			"city":"Newark",
			"department":"Marketing",
			"email":"doej@email.com"
		};
		assert.deepEqual(testData.emaswitch00.ports.A2, A2, 'emaswitch00 port A2 matches');
		
		assert.ok(testData.emaswitch01.ports.B1, 'emaswitch01 port B1 found');
		var B1 = {
            "id": 3,
			"firstName":"Ryan",
			"lastName":"Serva",
			"userName":"rserva",
			"city":"Newark",
			"department":"Software",
			"email":"rserva@email.com"
		};
		assert.deepEqual(testData.emaswitch01.ports.B1, B1, 'emaswitch01 port B1 matches');
		
		done();
	});
});

/*

QUnit.test('template', function(assert){
	
});

*/