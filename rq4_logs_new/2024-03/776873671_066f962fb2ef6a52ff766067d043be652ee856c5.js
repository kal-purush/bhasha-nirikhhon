var appTDEE = angular.module("appTDEE", []);


appTDEE.controller('TDEEController', TDEEController);
TDEEController.$inject = ["$scope", "$timeout"];
                   
function TDEEController($scope, $timeout) {
  // Contents of the calculation tab <select> elements.
  $scope.gender = {
    availableOptions: [
      {id: '1', name: 'Male'},
      {id: '2', name: 'Female'}
    ],
    selectedOption: {id: '1', name: 'Male'}
  };
  $scope.heightUnits = {
    availableOptions: [
      {id: '1', name: 'inches'},
      {id: '2', name: 'cm'}
    ],
    selectedOption: {id: '1', name: 'inches'}
  };
  $scope.weightUnits = {
    availableOptions: [
      {id: '1', name: 'lbs'},
      {id: '2', name: 'kg'}
    ],
    selectedOption: {id: '1', name: 'lbs'}
  };
  $scope.activitiesPerWeek = {
    availableOptions: [
      {id: '1', name: '0', multiplier: 1.2},
      {id: '2', name: '1-2', multiplier: 1.375},
      {id: '3', name: '3-5', multiplier: 1.55},
      {id: '4', name: '6-7', multiplier: 1.725}
    ],
    selectedOption: {id: '2', name: '1-2', multiplier: 1.375}
  }; 
  $scope.activityUnits = {
    availableOptions: [{id: '1', name: '/ week'}],
    selectedOption: {id: '1', name: '/ week'}
  };
  $scope.bodyfatUnits = {
    availableOptions: [{id: '1', name: '%'}],
    selectedOption: {id: '1', name: '%'}
  };
  
  // Default values for when the page loads.
  $scope.calcMethod = "mifflin";
  $scope.age = 30;
  $scope.height = 75;    
  $scope.weight = 200;   
  $scope.bodyfatPercentage = 25;  
  
  var LBS_TO_KG = 2.2;  // Ratio of lbs to kg.
  var CM_TO_IN = 2.54;  // Ratio of cm to inches.
  var CM_UNIT = "cm";
  var LBS_UNIT = "lbs";
  var IN_UNIT = "inches";
  var rounding = 100;
  
  $scope.calculateBMR = function() {
    var kgWeight = convertToMetric($scope.weightUnits.selectedOption.name, 
                                   LBS_UNIT, $scope.weight, LBS_TO_KG, true);
    
    var cmHeight = convertToMetric($scope.heightUnits.selectedOption.name, 
                                   IN_UNIT, $scope.height, CM_TO_IN, false);

    $scope.katch = calculateKatch(kgWeight);
    $scope.mifflin = calculateMifflin(kgWeight, cmHeight);
  }; 
  $scope.calculateBMR();
  
  function calculateKatch(kg) {
    var leanBodyMass = kg - (($scope.bodyfatPercentage / 100) * kg);
    return 370 + (21.6 * leanBodyMass);
  }
  
  function calculateMifflin(kg, cm) {
    var equation = (10 * kg) + (6.25 * cm) - (5 * $scope.age);
    return equation += ($scope.gender.selectedOption.name == "Male") ? 5 : -161;
  }
  
  $scope.convertWeight = function() {    
    var w = convertUnits($scope.weightUnits.selectedOption.name, 
                         LBS_UNIT, $scope.weight, LBS_TO_KG);
    $scope.weight = Math.round(w * rounding) / rounding;
  };
  
  $scope.convertHeight = function() {
    var h = convertUnits($scope.heightUnits.selectedOption.name, 
                         CM_UNIT, $scope.height, CM_TO_IN);
    $scope.height = Math.round(h * rounding) / rounding;
  };
  
  function convertUnits(unit, smallerUnit, value, ratio) {
    return (unit === smallerUnit) ? value * ratio : value / ratio;
  }
  
  function convertToMetric(unit, condition, value, ratio, divide) {
    if (divide)
      return (unit === condition) ? value / ratio : value; 
    else
      return (unit === condition) ? value * ratio : value;
  }
  
  var tabClass = "tabExtend";
  var infoClass = "infoExtend";
  var delay = 750;
  
  // function used to extend and retract the calculation tab
  $scope.tabExtended = "";
  $scope.extendTab = function() {    
    if ($scope.tabExtended == "") {
      $scope.tabExtended = tabClass;
    }
    else {
      if ($scope.infoExtended == infoClass) {
        $scope.infoExtended = "";
        $timeout(function() { $scope.tabExtended = ""; }, delay);
      }
      else {
        $scope.tabExtended = "";
      }
    }
  };
  
  // function used to extend and retract the info tab
  $scope.infoExtended = "";
  $scope.extendInfo = function() {
    if ($scope.infoExtended == "") {
      if ($scope.tabExtended == "") {
        $scope.tabExtended = tabClass;
        $timeout(function() { $scope.infoExtended = infoClass; }, delay);
      }
      else {
        $scope.infoExtended = infoClass;
      }
    } 
    else {
      $scope.infoExtended = "";
    }
  };
}

// Restrict the maximum character length of an input.
appTDEE.directive('uiMaxlength', function() {
  return {
    require: 'ngModel',
    link: function(scope, el, attrs, model) {
      var maxLength = parseInt(attrs.uiMaxlength, 10);

      var inputValueParser = function(value) {
        if(value.length > maxLength) {
          value = value.substring(0, maxLength);
          model.$setViewValue(value);
          model.$render();
        }
        return value;
      };
      model.$parsers.push(inputValueParser);
    }
  };
});

// Restrict the maximum value of an input.
appTDEE.directive('uiMaxvalue', function() {
  return {
    require: 'ngModel',
    link: function(scope, el, attrs, model) {
      var maxValue = parseInt(attrs.uiMaxvalue, 10);

      var inputValueParser = function(value) {
        if(value > maxValue) {
          value = maxValue;
          model.$setViewValue(value);
          model.$render();
        }
        return value;
      };
      model.$parsers.push(inputValueParser);
    }
  };
});

// Display the result of the BMR Calculation.
appTDEE.directive('bmrResult', function() {
  return {
    restrict: 'E',
    replace: true,
    template: function(element, attrs) {
      return '<p>BMR of <span>{{' + attrs.method + ' | number:0}}</span> calories / day</p>'
    }
  };
});

// Display the result of the TDEE calculation.
appTDEE.directive('tdeeResult', function() {
  return {
    restrict: 'E',
    replace: true,
    template: function(element, attrs) {
      return '<p>' + 
        'TDEE of <span>{{' + attrs.method + ' * activitiesPerWeek.selectedOption.multiplier | number:0}}</span> calories / day' + 
      '</p>'
    }
  };
});

// Display the TDEE range in the info card.
appTDEE.directive('tdeeRange', function() {
  return {
    restrict: 'E',
    controller: 'TDEEController',
    scope: {
      calcMethod: '@tdeeRange'
    },
    template: function(scope, attrs) {
      return '<p>'+
        '<span>{{($eval(calcMethod) * activitiesPerWeek.selectedOption.multiplier)' + attrs.lower + ' | number:0}}</span> to <span>{{($eval(calcMethod) * activitiesPerWeek.selectedOption.multiplier)' + attrs.upper + ' | number:0}}</span> calories / day' + 
      '</p>'
    }
  };
});

appTDEE.directive('textImgLink', function() {
  return {
    restrict: 'E',
    replace: true,
    template: function(scope, attrs) {
      return '<div>' +
          '<a href="' + attrs.url + '">' +
            '<p>' + attrs.text + '</p>' +
            '<img src="' + attrs.imgsrc + '" />' +
          '</a>' +
        '</div>'
    }
  }
});

appTDEE.directive('textIconLink', function() {
  return {
    restrict: 'E',
    replace: true,
    template: function(scope, attrs) {
      return '<div>' +
          '<a href="' + attrs.url + '">' +
            '<p>' + attrs.text + '</p>' +
            '<i class="fa fa-' + attrs.icon + '" aria-hidden="true"></i>' +
          '</a>' +
        '</div>'
    }
  }
});

