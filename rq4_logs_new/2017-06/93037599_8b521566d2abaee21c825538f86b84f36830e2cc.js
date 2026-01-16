

var myApp = angular.module('myApp', ['ngAnimate'])
myApp.controller('mainCtrl', function($scope, $timeout) {
  vm = this;
  vm.filter = 'all';
  vm.deletetransitionReset =function(){
    if(vm.filter == 'all' || vm.filter == 'deletetransitions')
  {vm.items =[
        {name: "Lunchmeat"},
        {name: "Bread"},
        {name: "Milk"},
        {name: "Mustard"},
        {name: "Cheese"}
        ];}
  }
   vm.items = [
        {name: "Lunchmeat"},
        {name: "Bread"},
        {name: "Milk"},
        {name: "Mustard"},
        {name: "Cheese"}
        ];

vm.descriptiondata=[
    {
        "name": "statusone",
        "title": "Success",
        "description": "Use the below code to get checkmark animation."
    },
    {
        "name": "statustwo",
        "title": "Error",
        "description": "Use the below code to get Shake(Error) animation."
    },
    {
        "name": "statusthree",
        "title": "Warning",
        "description": "Use the below code to get Blink(warning) animation."
    },
    {
        "name": "fadeIn",
        "title": "Fade In",
        "description": "Use the class name 'fadeIn' for your element and add the below css to get fadeIn animation."
    },
    {
         "name": "fadeOut",
        "title": "Fade Out",
        "description": "Use the class name 'fadeOut' for your element and add the below css to get fadeOut animation."
    },
    {
         "name": "fadeInLeft",
        "title": "Fade In Left",
        "description": "Use the class name 'fadeInLeft' for your element and add the below css to get Fade In Left animation."
    },
    {
         "name": "fadeInRight",
        "title": "Fade In Right",
        "description": "Use the class name 'fadeInRight' for your element and add the below css to get Fade In Right animation."
    },
    {
        "name": "fadeInUp",
        "title": "Fade In Up",
        "description": "Use the class name 'fadeInUp' for your element and add the below css to get Fade In Up animation."
    },
    {
         "name": "fadeInDown",
        "title": "Fade In Down",
        "description": "Use the class name 'fadeInDown' for your element and add the below css to get Fade In Down animation."
    },
    {
         "name": "slideInDown",
        "title": "slide Down",
        "description": "Use the class name 'slideInDown' for your element and add the below css to get Slide In Down animation."
    },
    {
         "name": "slideInUp",
        "title": "slide Up",
        "description": "Use the class name 'slideInUp' for your element and add the below css to get Slide In Up animation."
    },
    {
        "name": "slideInLeft",
        "title": "slide In Left",
        "description": "Use the class name 'slideInLeft' for your element and add the below css to get slide In Left animation."
    },
    {
         "name": "slideInRight",
        "title": "slide In Right",
        "description": "Use the class name 'slideInRight' for your element and add the below css to get slide In Right animation."
    },
    {
         "name": "carousel",
        "title": "Carousel",
        "description": "Use the below code to get the carousel side effect. Need to use 'ngAnimate' file to get the slide animation. This works only when two elements are displayed based on condition with 'ng-if' directive in angular. And use the classes to the appropriate elements. Add the controller to parent element."
    },
    {
         "name": "deleteTransition",
        "title": "Delete Transitions",
        "description": "It deletes the selected element with smooth animation. Use the below code"
    },
    {
         "name": "dropdown",
        "title": "Dropdown",
        "description": "It's a simple dropdown effect with dropdown list.",        
        "disclaimer" : "Disclaimer",
        "note" : "As the accordion is dealing with height we required some additional work"
    },
    {
         "name": "accordion",
        "title": "Accordion",
        "description": "It's a simple accordion effect with adding and removing a class to parent element."
    }
];


$scope.successClick = function ($scope){
         angular.element(document.querySelector("#draw")).addClass("draw");
         $timeout(function() {
            angular.element(document.querySelector("#draw")).removeClass("draw");
          }, 100);
          $timeout(function() {
            angular.element(document.querySelector("#draw")).addClass("draw");
          }, 150); 
    } 

$scope.errorClick = function ($scope){
         angular.element(document.querySelector("#shake")).addClass("shake");
         $timeout(function() {
            angular.element(document.querySelector("#shake")).removeClass("shake");
          }, 100);
          $timeout(function() {
            angular.element(document.querySelector("#shake")).addClass("shake");
          }, 150); 
    } 

$scope.warningClick = function ($scope){
         angular.element(document.querySelector("#blink")).addClass("blink");
         $timeout(function() {
            angular.element(document.querySelector("#blink")).removeClass("blink");
          }, 100);
          $timeout(function() {
            angular.element(document.querySelector("#blink")).addClass("blink");
          }, 150); 
    } 

    $scope.fadeoutClick = function ($scope){
         angular.element(document.querySelector("#fadeOut")).addClass("fadeOut");
         $timeout(function() {
            angular.element(document.querySelector("#fadeOut")).removeClass("fadeOut");
          }, 1000);
    } 
    $scope.fadeInClick = function ($scope){
         angular.element(document.querySelector("#fadeFirstElement")).addClass("fadeIn");
         $timeout(function() {
            angular.element(document.querySelector("#fadeFirstElement")).removeClass("fadeIn");
          }, 1000);
    } 
    $scope.fadeInLeftClick = function ($scope){
         angular.element(document.querySelector("#fadeinLeft")).addClass("fadeInLeft");
         $timeout(function() {
            angular.element(document.querySelector("#fadeinLeft")).removeClass("fadeInLeft");
          }, 1000);
    } 
    $scope.fadeInRightClick = function ($scope){
         angular.element(document.querySelector("#fadeinRight")).addClass("fadeInRight");
         $timeout(function() {
            angular.element(document.querySelector("#fadeinRight")).removeClass("fadeInRight");
          }, 1000);
    } 
    $scope.fadeInUpClick = function ($scope){
         angular.element(document.querySelector("#fadeinUp")).addClass("fadeInUp");
         $timeout(function() {
            angular.element(document.querySelector("#fadeinUp")).removeClass("fadeInUp");
          }, 1000);
    } 
    $scope.fadeInDownClick = function ($scope){
         angular.element(document.querySelector("#fadeinDown")).addClass("fadeInDown");
         $timeout(function() {
            angular.element(document.querySelector("#fadeinDown")).removeClass("fadeInDown");
          }, 1000);
    } 


$scope.slideInDownClick = function ($scope){
         angular.element(document.querySelector("#slideFirstElement")).addClass("slideInDown ");
         $timeout(function() {
            angular.element(document.querySelector("#slideFirstElement")).removeClass("slideInDown ");
          }, 1000);
    } 

$scope.slideInUpClick = function ($scope){
         angular.element(document.querySelector("#slideinUp")).addClass("slideInUp");
         $timeout(function() {
            angular.element(document.querySelector("#slideinUp")).removeClass("slideInUp");
          }, 1000);
    } 

$scope.slideInLeftClick = function ($scope){
         angular.element(document.querySelector("#slideinLeft")).addClass("slideInLeft ");
         $timeout(function() {
            angular.element(document.querySelector("#slideinLeft")).removeClass("slideInLeft ");
          }, 1000);
    } 

$scope.slideInRightClick = function ($scope){
         angular.element(document.querySelector("#slideinRight")).addClass("slideInRight ");
         $timeout(function() {
            angular.element(document.querySelector("#slideinRight")).removeClass("slideInRight ");
          }, 1000);
    } 


    vm.removeItem = function(index) {   

           vm.items.splice(index, 1);          
            
    };
    vm.getcode=function(id){
      vm.getcodeID=id;
angular.forEach(vm.descriptiondata,function(value){
  if(value.name == id)
  {
    vm.heading=value.name;
    vm.title=value.title;
    vm.description= value.description;
    vm.disclaimer= value.disclaimer;
    vm.note= value.note;
  }
})
    }
    vm.copy=function(id)
    {
      document.getElementById(id).select();
      document.execCommand('copy');
    }

});

myApp.controller('animationsCtrl', function($scope) {
  $scope.fadeAnimation = true;
  $scope.toggle = false;
$scope.toggle1 = false;
$scope.dropdown = false;
});

var selector, elems, makeActive;

selector = '.listitem';

elems = document.querySelectorAll(selector);

makeActive = function () {
    for (var i = 0; i < elems.length; i++)
        elems[i].classList.remove('selected');
    
    this.classList.add('selected');
};

for (var i = 0; i < elems.length; i++)
    elems[i].addEventListener('mousedown', makeActive);

function statusSelected() {
 // alert('hi');
 document.getElementById('slideFirstElement').classList.remove('selected'); 
 document.getElementById('statusFirstElement').classList.remove('selected'); 
 document.getElementById('fadeFirstElement').classList.remove('selected'); 
    document.getElementById('statusFirstElement').classList.add('selected');
}
function fadeSelected() {
 // alert('hi');
 document.getElementById('slideFirstElement').classList.remove('selected'); 
 document.getElementById('statusFirstElement').classList.remove('selected'); 
 document.getElementById('fadeFirstElement').classList.remove('selected'); 
    document.getElementById('fadeFirstElement').classList.add('selected');
}
function slideSelected() {
 // alert('hi');
 document.getElementById('slideFirstElement').classList.remove('selected'); 
 document.getElementById('statusFirstElement').classList.remove('selected'); 
 document.getElementById('fadeFirstElement').classList.remove('selected'); 
    document.getElementById('slideFirstElement').classList.add('selected');
}

myApp.controller('dataController', ['$scope', '$window', '$timeout', function($scope) {

   
}]);

myApp.controller('dragDropCtrl', function($scope) {



});