//
// (c) 2016 Appirio, Inc.
//
// JS Controller: PC_CaseListcontroller
// Description: To perform the actions related to PC_CaseList.cmp
//
// 29th November 2016    Meghna Vijay    Original (Task # T-557145)
// 
({
  doInit: function(component, event, helper) {
      	helper.setFilterOptions(component,event,helper);
      	helper.getHeaderLabels(component,event,helper);
        helper.updateCaseList(component);
    },
    sortByDate: function(component, event, helper) {
        var urlEvent = $A.get("e.force:navigateToURL");
        console.log('urlEvent'+urlEvent);
        urlEvent.setParams({
          "url": '/calendar'
        });
        urlEvent.fire();

    },
    search: function(component, event, helper) {
        component.set("v.isNotSearch", false);
        component.set("v.isSearchByDate",false);
        component.set("v.pageNumber",0);
        var searchText = component.get("v.val");
        component.set("v.fetchedAllRecords",false);

        helper.updateCaseList(component);
    },
    navigateToDetail: function(component, event, helper) {
        var obj = event.currentTarget.dataset.index;
        for(var i = 0;i<component.get('v.displayCaseList')[obj].length;i++){
            var temp = component.get('v.displayCaseList')[obj][i];
            if(temp.label == 'Id'){
                component.set('v.recordId',temp.value);
            }
        }
        debugger;
    },
    sortOptionChanged : function(component, event, helper){
        component.set("v.pageNumber",0);
        component.set("v.fetchedAllRecords",false);
  
        var searchByDate = component.get("v.isSearchByDate");
        if(!searchByDate){
           helper.updateCaseList(component,false);
        }
    },
    handleCapture : function(component,event,helper){
		console.log('Event Captured');
        helper.nextPage(component);        
    },
    toggleSortDirection : function(component,event,helper){
        var sortElement1 = component.find('sortDirection');
        $A.util.toggleClass(sortElement1, "fa fa-arrow-down");
        component.set('v.isSearchByDate',false);
        component.set("v.pageNumber",0);
        component.set("v.fetchedAllRecords",false);
        if(component.get("v.sortDirection") !== 'A-Z'){
            component.set("v.sortDirection",'A-Z');
        }
        else{
            component.set("v.sortDirection",'Z-A');
        }
        helper.updateCaseList(component);
    	
	},
    toggleSpinner : function(component, event, helper){
        helper.toggle(component);
    }
    
})