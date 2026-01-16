({
    doInit : function (component) {
        var action = component.get('c.getPicklistforQuery');
        action.setParams({
            "isQuery": component.get('v.isQuery'),
            "dataSource": component.get('v.dataSource'),
            "allowNull": component.get('v.allowNull'),
            "queryString": component.get('v.queryString'),
            "objectName": component.get('v.objectName'),
            "fieldName": component.get('v.fieldName'),
            "labelField": component.get('v.labelField'),
            "valueField": component.get('v.valueField')
        });
        action.setCallback(this, function (response) {
            var list = response.getReturnValue();
            component.set('v.picklistValues', list);
        });
        $A.enqueueAction(action);
    }
})