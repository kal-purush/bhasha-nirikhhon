function cegaLarga(n, maxLoop){	
    maxLoop == null ?
        maxLoop = Infinity :
        void(null);
    
    var noh = new Object();
    for( let k in n){
        noh[k] = n[k];
    }
    
    var nozes = new Array();
    nozes.push(noh);    
    var hashes = new Set();
    hashes.add(noh.hsh);
    
	for(let i = 0; i < maxLoop; i++){
        
        nozes[i].larga = i;
        
		if(nozes[i].hsh == "\nabcd\n"){
			return nozes[i];
		}else{
			nozes[i].filhos = geraFilhos(nozes[i]);
            nozes[i].filhos.forEach(function(e,e,s){
                if( ! hashes.has(e.hsh)){
                    nozes.push(e);
                    hashes.add(e.hsh);
                }
            });
		}
	}
}