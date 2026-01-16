 
function numberBoxInit (idElement,idAttrName, widthElement) {
	 $("#"+idElement).dxNumberBox({
        value: 15,
        min: 10,
        max: 100,
        showSpinButtons: true,
        inputAttr: {
   		 id: idAttrName,
   		 class:"input-field__value" 
   		},
   		width: widthElement
    });
};

numberBoxInit ("input-field__cargoPlace","input-field__cargoPlace-id", 128);