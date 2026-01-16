function getCompValues(){
	let cName = document.getElementById("comp_name").value;
	let cDesc = document.getElementById("comp_desc").value;

    if(cName != ''){
        let newComp = new component(cName, cDesc);
        components.push(newComp);
        populateComponents();
        setOrderComponents();
    }
		
	document.getElementById("comp_name").value = '';
	document.getElementById("comp_desc").value = '';
}


function addOrder(){
	let oName = document.getElementById('order_name').value;
    if(oName !=''){
        let newOrder = new orders(oName);
        let comp1 = document.getElementById('selectOne').value;
        let mod1 = document.getElementById('selectModOne').value;
        
  
        
        let tempComp = new component(comp1, '');
        //tempComp.setRequiredMod(mod1);
        console.log("Comp " + comp1 + " mod1 " + mod1);
        
        ordersList.push(newOrder);
    }
}

var testList = ['1', '2', '3', '4', '5', '6'];

function setOrderComponents(){
    
	$('.orderComps').each(function(){
        $(this).empty();
		var ords = $(this)[0];	
		for(var i=0; i<components.length; i++){ 
			let opt = components[i].name;
			let el = document.createElement('option');
			el.textContent = opt;
            
			ords.appendChild(el);
            $(this).children().eq(i).data("comp", components[i]);
            
		}
	});
}
