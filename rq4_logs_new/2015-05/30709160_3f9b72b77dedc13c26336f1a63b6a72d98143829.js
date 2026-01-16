var ProviderApp = angular.module('ProviderApp', ['ui.grid','ui.grid.resizeColumns','ui.grid.pagination','ngResource','ui.grid.selection','ui.grid.exporter']);  // note you can add multiple injectors ['ui.grid','blah']

ProviderApp.controller('ProductChangesCtrl',function ($scope, $http, uiGridConstants) {
	 
	$http.get('/FCARest/dash/annuity/prodchanges').success(function(data)  {
			$scope.changes = data; 
			
			$scope.sortField = 'changeDate';
			$scope.reverse = true;
	});
});	 // Product Changes Controller


ProviderApp.controller('ProductOptionsCtrl',function ($scope, $http, uiGridConstants) {
	 
	$http.get('/FCARest/dash/annuity/annuityoptions').success(function(data)  {
			$scope.options = data; 
			
			$scope.sortField = 'provider';
			$scope.reverse = false;
			
			$scope.formData = {
		          enhanced : 1,
		          impaired : 1,
		          escalating : 1,
		          deferred : 0,
		          advised : 1,
		          single : 1,
		          joint : 1
		    };
	});
});	 // Product Options Controller

ProviderApp.controller('FeatureOptionsCtrl',function ($scope, $http, uiGridConstants) {

	 $scope.columns =[{ field: 'provider', cellClass: 'TableCell', headerCellClass: 'TableHeader', displayName: 'Provider'}, 
	                  { field: 'product', cellClass: 'TableCell', headerCellClass: 'TableHeader', displayName: 'Product'},
	                  { field: 'subCategory', cellClass: 'TableCell', headerCellClass: 'TableHeader', displayName: 'Category'},
	                  { field: 'feature', cellClass: 'TableCell', headerCellClass: 'TableHeader', displayName: 'Feature'},
	                  {field: 'featureDate', cellClass: 'TableCell', headerCellClass: 'TableHeader', displayName: 'Date', enableFiltering:false, width: 80, sort: {direction: uiGridConstants.DESC, priority: 1}},
	                  { field: 'currentValue', cellClass: 'TableCell', headerCellClass: 'TableHeader', displayName: 'Current Value'},
	                  { field: 'previousValue', cellClass: 'TableCell', headerCellClass: 'TableHeader', displayName: 'Previous Value'},
	                  ];
	 
	 $scope.gridOptions = {
			 enableSorting: true,
			 enablePaginationControls: false,
			 paginationPageSizes: [6,12,18],
			 paginationPageSize: 200,
			 enableFiltering: false,
			 enableHorizontalScrollbar: true,
			 enableVerticalScrollbar: true,
			 enableScrollbars: true, 
			 rowHeight: 25,
			 exporterCsvFilename: 'features.csv',
			 columnDefs: $scope.columns
	 };
	 
	 $scope.gridOptions.enableHorizontalScrollbar = uiGridConstants.scrollbars.NEVER;
	//  $scope.gridOptions.enableVerticalScrollbar = uiGridConstants.scrollbars.NEVER;
	 
	$scope.export  = function() {
			$scope.gridApi.exporter.csvExport('all','all');	
	} ; 
	 
	$scope.gridOptions.onRegisterApi = function (gridApi) {
			 $scope.gridApi = gridApi;
	 };
	 
	$http.get('/FCARest/dash/annuity/featurechanges').success(function(data)  {
			$scope.gridOptions.data = data; //.provider; removed as no longer required with POJO mapping in place.	
	});
	
	
});	 

ProviderApp.controller('TopFeatureOptionsCtrl',function ($scope, $http, uiGridConstants) {

	 $scope.columns =[{ field: 'subCategory', cellClass: 'TableCell', headerCellClass: 'TableHeader', displayName: 'Category', enableFiltering:false},
	                  { field: 'feature', cellClass: 'TableCell', headerCellClass: 'TableHeader', displayName: 'Feature'},
	                  { field: 'changeCount', cellClass: 'TableCell', headerCellClass: 'TableHeader', displayName: 'No Of Changes', enableFiltering:false},
	                  ];
	 
	 $scope.gridOptions = {
			 enableSorting: true,
			 enablePaginationControls: false,
			 paginationPageSizes: [8,16,24],
			 paginationPageSize: 8,
			 enableFiltering: true,
			 enableHorizontalScrollbar: false,
			 enableScrollbars: false, 
			 rowHeight: 22,
			 columnDefs: $scope.columns
	 }
	 
	 $scope.gridOptions.enableHorizontalScrollbar = uiGridConstants.scrollbars.NEVER;
	 $scope.gridOptions.enableVerticalScrollbar = uiGridConstants.scrollbars.NEVER;
	 
	$scope.gridOptions.onRegisterApi = function (gridApi) {
			 $scope.gridApi = gridApi;
	 }
	 
	$http.get('/FCARest/dash/annuity/topfeaturechanges').success(function(data)  {
			$scope.gridOptions.data = data; //.provider; removed as no longer required with POJO mapping in place.	
	});
});	 

// -----------------------------------------------------------------------------------------
// Google Charts JS
//-----------------------------------------------------------------------------------------

google.load("visualization", "1.1", {packages:["bar","corechart"]});
google.setOnLoadCallback(drawChart);


function drawChart() {
  var data = new google.visualization.DataTable();
  
  data.addColumn('string', 'Provider');
  data.addColumn('number', 'Products');

  var options = {
	title: "",
	titleTextStyle: {fontSize: 14, fontName: 'Lato', color: 'black', bold: true},
    //chart: {
    //  	subtitle: 'Jan 23rd 2015',
    //},
    legend: {position: "none"},
    vAxis: { title: "", position: "none"},
    hAxis: {title: "Number Of Products", viewWindow:{max:2, min:0}, titleTextStyle: {fontSize: 11, fontName: 'Lato'}},
    chxs : '0N*f0*',
    bars: 'horizontal', // Required for Material Bar Charts.
  };

  var chart = new google.charts.Bar(document.getElementById('barchart_material'));

  // Get JSON From Web Service
  var rawjsonData = $.ajax({
      url: "/FCARest/dash/annuity/providercnt",
      dataType:"json",
      async: false
      }).responseText; 
  
  // parse JSON into Javascript Object
  var jsonData = JSON.parse(rawjsonData);
  
  // Process Javascript Object and add rows to data
  for(var i = 0; i < jsonData.length; i++) {
	    var obj = jsonData[i]; 
	    // console.log(obj.provider);
	    // console.log(obj.productCount);
	    data.addRows([[obj.provider, Number(obj.productCount)]]);
	}
  
  chart.draw(data, google.charts.Bar.convertOptions(options));
  
}

//-----------------------------------------COMBO CHART

google.setOnLoadCallback(drawVisualization);

function drawVisualization() {
  // Some raw data (not necessarily accurate)
  var data = new google.visualization.DataTable();
  
  var customClassNames = {
		'titleTextStyle' : 'TableHeader'  
  };
  
  data.addColumn('string', 'Month');
  data.addColumn('number', 'Product Count');
  data.addColumn('number', 'Provider Count');

  var options = {
    titleTextStyle: {fontSize: 14, fontName: 'Lato'},
    animation: { startup: true, duration: 1000},
    vAxis: {title: ""},
    hAxis : { format: 'MMM yy'},
    seriesType: "bars",
    //cssClassNames : customClassNames,
    series: {1: {type: "line"}}
  };
  
  // Get JSON From Web Service
  var rawjsonData = $.ajax({
      url: "/FCARest/dash/annuity/prodprovtime",
      dataType:"json",
      async: false
      }).responseText; 
  
  // parse JSON into Javascript Object
  var jsonData = JSON.parse(rawjsonData);
  var monnames = new Array("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec");
  
  // Process Javascript Object and add rows to data
  for(var i = 0; i < jsonData.length; i++) {
	    var obj = jsonData[i];
	    //console.log(obj.yearMonth); Cn't use date as chart is created as a discrete chart
	    var dateobj = new Date(obj.yearMonth);
	    var datestr = monnames[dateobj.getMonth()] + " " + dateobj.getFullYear();
	    // console.log(datestr);
	    data.addRows([[datestr, Number(obj.productCount), Number(obj.providerCount)]]);
	}
  
  var chart = new google.visualization.ComboChart(document.getElementById('chart_div'));
  chart.draw(data, options);
  
}


 

