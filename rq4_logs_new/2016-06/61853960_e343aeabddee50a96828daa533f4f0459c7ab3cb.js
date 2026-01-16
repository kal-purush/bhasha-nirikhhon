({
	btnClick : function(component, event, helper) {
		var target = event.getSource();
        var lang = target.get("v.label");
        helper.changeLang(component, lang);
        window.location.reload();
	}
})