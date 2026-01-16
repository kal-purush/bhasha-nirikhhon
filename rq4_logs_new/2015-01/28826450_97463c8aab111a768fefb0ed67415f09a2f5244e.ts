///<reference path='Customer.ts' />
///<reference path='typings/angularjs/angular.d.ts' />
var queue = new PubSub.PubSubQueue();

var cs = new Vantage.CustomerStore(queue); 

var c = new Vantage.Customer(); 
c.firstName = "bob";
c.homePhone = "1234";
c.ID = 1; 
c.lastName = "fred";
c.workPhone = '4567'; 

queue.Publish(Vantage.Topics.NewCustomers().request, c);   

class AddCustomers {
    
    constructor($scope) {
        $scope.NewCustomer = () => {
            this.NewCustomer($scope);
        }
    }
    public NewCustomer(scope : any) {
        var c = new Vantage.Customer(); 
        c.firstName = scope.firstName;
        c.lastName = scope.lastName; 
        c.homePhone = scope.homePhone; 
        queue.Publish(Vantage.Topics.NewCustomers().request, c); 
    }
}

angular.module('App',[]).controller('Customers', ($scope) => {
    //$scope.customers = [{ firstName: "HI" }]; 
    queue.AddSubscription(Vantage.Topics.GetCustomer().response, (ret) => {
        $scope.customers = ret; 
    });    
    queue.Publish(Vantage.Topics.GetCustomer().request, {}); 
}).controller('AddCustomers', AddCustomers);




//angular.module('App', []).controller('AddCustomers', AddCustomers );