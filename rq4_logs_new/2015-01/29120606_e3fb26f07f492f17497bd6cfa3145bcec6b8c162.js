function SalaryCalculator(){
            var data ={
                basic : 0,
                hra : 0,
                da : 0,
                tax : 0,
                salary : 0
            };
            this.get = function(attrName){
                return data[attrName];
            };
            this.set = function(attrName,value){
                data[attrName] = value;
                triggerChange(attrName);
            }
            var subscriptions = {};
            this.addSubscriber = function(attrName, subscriptionFn){
                subscriptions[attrName] = subscriptions[attrName] || [];
                subscriptions[attrName].push(subscriptionFn);
            };
            this.removeSubscriber = function(){
                //home work
            };
            var self = this;
            function triggerChange(attrName){
                var subscriptionFns = subscriptions[attrName] || [];
                for(var i=0;i<subscriptionFns.length;i++){
                    subscriptionFns[i].call(self);
                }
            }
        }
        SalaryCalculator.prototype.calculate = function(){
            var gross = this.get('basic') + this.get('hra') + this.get('da');
            var net = gross * ((100-this.get('tax'))/100);
            this.set('salary', net);

        }