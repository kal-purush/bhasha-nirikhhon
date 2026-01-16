var BUDGET_CONTROLLER= (function(){
    var Expenses=function(id,description,value){
            this.id=id;
            this.description=description;
            this.value=value;
            this.percentage=-1
        
    }
    Expenses.prototype.calcPercentage=function(totalIncome) {
        if (totalIncome > 0 ){
            this.percentage=Math.round((this.value/totalIncome)*100)
        } else{
            this.percentage=-1
        }   
    }
    Expenses.prototype.getPercentage= function () {
        return this.percentage;
    }
    var Income=function(id,description,value){
        this.id=id;
        this.description=description;
        this.value=value;}

        function calculateTotal(type){
            var sum =0;
            data.allItems[type].forEach((el,index,arr)=>{
                sum +=el.value
            })
            data.totals[type]= sum 
        }

        var data={
            allItems:{
                exp:[],
                inc:[]
            },
            totals:{
                exp:0,
                inc:0
            },
            budget:0,
            percentage:-1,
        }

        

        return{
            addItem:function(type ,des,val){
                var newItem,ID;
                if(data.allItems[type].length>0){
                    ID=data.allItems[type][data.allItems[type].length -1].id +1
                }else{
                    ID=0
                }
                if(type==='exp'){
                    newItem=new Expenses(ID,des,val)
                }
                else if(type==='inc'){
                    newItem=new Income(ID,des,val)
                }
                data.allItems[type].push(newItem)
                return newItem
            },
            deleteItem:function(type,id){
                var ids,index
                ids=data.allItems[type].map(function(value,index,arr) {
                    return value.id;
                })
                index =ids.indexOf(id)
                if(index !== -1){
                    data.allItems[type].splice(index,1)
                }
                
            },
            calculateBudget:function(){
                calculateTotal('exp')
                calculateTotal('inc')
                data.budget=data.totals.inc-data.totals.exp 
                if(data.totals.inc >0){
                data.percentage=Math.round((data.totals.exp/data.totals.inc)*100)
                }else{
                    data.percentage=-1;
                }
            },
            calculatePercentages:function() {
                data.allItems.exp.forEach(function(element,index,arr) {
                    element.calcPercentage(data.totals.inc)
                }) 
            },
            getPercentage:function() {
                var allPercentages=data.allItems.exp.map(function(element,index,arr) {
                    return element.getPercentage()
                })
                return allPercentages
            },
            getBudget:function(){
                return{
                    budget:data.budget,
                    totalIncome:data.totals.inc,
                    totalExpenses:data.totals.exp,
                    percentage:data.percentage
                }
            },

            testing:function(){
                console.log(data)
            }
        };

})();










var UI_CONTROLLER =(function(){
    var DOMStrings={
        type:'.add__type',
        description:'.add__description',
        value:'.add__value',
        inputBtn:'.add__btn',
        incomeContainer:'.income__list',
        expensesContainer:'.expenses__list',
        budgetLabel:'.budget__value',
        incomeLabel:'.budget__income--value',
        expensesLabel:'.budget__expenses--value',
        percentageLabel:'.budget__expenses--percentage',
        eventContainer:'.container',
        expensesPercentageLabel:'.item__percentage',
        monthLabel:'.budget__title--month'
    }

    function formatNumber(num,type){
        var numSplit,int,decimal,sign;
        num=Math.abs(num)
        num=num.toFixed(2)
        numSplit=num.split('.')
        
        int =numSplit[0]
        
        if(int.length >3){
        int =int.substr(0,int.length -3) + ',' + int.substr(int.length -3,3)
        }
        decimal=numSplit[1]
        type==='exp' ? sign='-':'+'
        return (type==='exp'? '-':'+')+' '+ int+'.'+ decimal
        }
    return{
        getInput: function(){
           return{
             type:document.querySelector(DOMStrings.type).value,
             description:document.querySelector(DOMStrings.description).value,
             value:parseFloat(document.querySelector(DOMStrings.value).value)
        }},
        addListItem:function(obj,type){
            var html,newHtml,element;
            if(type==='inc'){
            element=DOMStrings.incomeContainer;
            html= '<div class="item clearfix"id="inc-%id%"><div class="item__description">%description%</div><div class="right clearfix"><div class="item__value">%value%</div><div class="item__delete"><button class="item__delete--btn"><i class="ion-ios-close-outline"></i></button></div></div></div>' 
            }
            else if(type==='exp'){
            element=DOMStrings.expensesContainer;
            html ='<div class="item clearfix" id="exp-%id%"><div class="item__description">%description%</div><div class="right clearfix"><div class="item__value">%value%</div><div class="item__percentage">21%</div><div class="item__delete"><button class="item__delete--btn"><i class="ion-ios-close-outline"></i></button></div></div></div>'
            }
            newHtml=html.replace('%id%',obj.id)
            newHtml=newHtml.replace('%description%',obj.description)
            newHtml=newHtml.replace('%value%',formatNumber(obj.value,type));

            document.querySelector(element).insertAdjacentHTML('beforeend',newHtml)
        },
            deleteListItem:function(selectorID){
            var el=document.getElementById(selectorID)
            el.parentNode.removeChild(el)

        },
        displayBudget:function(obj){
            var type; 
            obj.budget > 0 ? type='inc' : type= 'exp'
            document.querySelector(DOMStrings.budgetLabel).textContent=formatNumber(obj.budget,type)
            document.querySelector(DOMStrings.incomeLabel).textContent=formatNumber(obj.totalIncome,'inc')
            document.querySelector(DOMStrings.expensesLabel).textContent=formatNumber(obj.totalExpenses,'exp')
        
            if (obj.percentage >0){
                document.querySelector(DOMStrings.percentageLabel).textContent=obj.percentage + '%'
            }else{
                document.querySelector(DOMStrings.percentageLabel).textContent='--'
            }

        },
        clearFields:function(){
            var fields,fieldsArray;
            fields = document.querySelectorAll(DOMStrings.description+ ',' +DOMStrings.value)
            fieldsArray =Array.prototype.slice.call(fields)
            fieldsArray.forEach((el,index,array) => {
                el.value="";
                
            });
            fieldsArray[0].focus()
        },
        dispayPercentages:function(percentages){
            var fields=document.querySelectorAll(DOMStrings.expensesPercentageLabel);
            var nodeListForEach=function(list,callback){
                for(let i=0;i<list.length;i++){
                    callback(list[i],i)
                }
            }
            nodeListForEach(fields,function(current,index){
                if(percentages[index] > 0){
                    current.textContent=percentages[index] +'%'
                }
                else{
                    current.textContent='--'
                }
                
            })
        },
       displayMonth:function(){
        const monthNames = ["January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
      ];
           var now,year,month;
        var now =new Date();
        year=now.getFullYear()
        month=monthNames[now.getMonth()]
        document.querySelector(DOMStrings.monthLabel).textContent= year + ' ' +month
       },
        getDOMStrings:function(){
            return DOMStrings
        }
    }
})()


var APPCONTROLLER=(function(budgetCtrl,UICtrl){
    function setupEventListners(){
        var DOM=UICtrl.getDOMStrings()
        document.querySelector(DOM.inputBtn).addEventListener('click',function (){
            ctrlAddItem()   
        })
        document.addEventListener('keypress',function(event){
            if(event.keyCode===13 ||event.keyCode===13 ){
                ctrlAddItem()
            }
        });
        document.querySelector(DOM.eventContainer).addEventListener('click',controlDeleteItem)
    }

    function updatePercentages(){
        budgetCtrl.calculatePercentages()
        var percentages =budgetCtrl.getPercentage()
        UICtrl.dispayPercentages(percentages)

    }

    function updateBudget(){
        budgetCtrl.calculateBudget()
        var budget=budgetCtrl.getBudget()
        UICtrl.displayBudget(budget)
    }
   

    function ctrlAddItem(){
        var newItem,input,budget;
        input= UICtrl.getInput()
        if(input.description!=='' && !isNaN(input.value) && input.value>0 ){
            newItem= budgetCtrl.addItem(input.type,input.description,input.value)
            UICtrl.addListItem(newItem,input.type)
            UICtrl.clearFields()

            updateBudget()

            updatePercentages()
        }
    };

    function controlDeleteItem(event){
        var itemID,splitID,type,ID;
        itemID=event.target.parentNode.parentNode.parentNode.parentNode.id;
        if(itemID){
            splitID=itemID.split('-')
            type=splitID[0]
            ID=parseInt(splitID[1])
            budgetCtrl.deleteItem(type,ID)
            UICtrl.deleteListItem(itemID)
            updateBudget()

            updatePercentages()
        }

    }

    

   return{
       init:function(){
           console.log('WELCOME')
           UICtrl.displayMonth()
           UICtrl.displayBudget({
                budget:0,
                totalIncome:0,
                totalExpenses:0,
                percentage:-1
            
           })
           setupEventListners()

       }
   }
    
})(BUDGET_CONTROLLER,UI_CONTROLLER)


APPCONTROLLER.init()