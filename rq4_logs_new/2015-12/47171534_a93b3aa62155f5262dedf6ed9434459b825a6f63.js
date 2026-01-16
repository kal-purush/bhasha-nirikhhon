main_module.controller('controllerAdd',function($scope,dataViewFactory,$location,Flash){
    
    $scope.name = "";
    $scope.address = "";
    $scope.age = 0;
    
    $scope.addClicked = function(){
                
        var temp = {
            name:$scope.name,
            address:$scope.address,
            age:$scope.age
        }
        
        if((temp.name.length === 0)|| (temp.address.length === 0) || (temp.age.length === 0)){
            console.log("please fill all the fields!");
            Flash.create('warning', 'Please fill all the fields!', 'custom-class');
            return;
        }        
        
        $('#add-btn').attr("disabled", true);
        
        var waitPromise = dataViewFactory.addPersonData(temp);
        //wait the response from server
        waitPromise.then(function(data){
        
            $scope.name = "";
            $scope.address = "";
            $scope.age = "";
            $('#add-btn').attr("disabled", false);
            
            //clear friends array to get udated data from backend when exit button is clicked
            dataViewFactory.friendsArray = [];
            
            Flash.create('success', 'New friend added!', 'custom-class');
            
        },function error(data){  
            
            $('#add-btn').attr("disabled", false);  
            Flash.create('warning', 'Failed to add friend!', 'custom-class');
        });
    }
    
    $scope.exitClicked = function(){
                
        $location.path('/list');
    }
});