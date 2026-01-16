var nm1 = ["b","c","ch","d","g","h","k","l","m","n","p","r","s","t","th","v","x","y","z","","","","",""];
var nm2 = ["a","e","i","o","u"];
var nm3 = ["b","bb","br","c","cc","ch","cr","d","dr","g","gn","gr","l","ll","lr","lm","ln","lv","m","n","nd","ng","nk","nn","nr","nv","nz","ph","s","str","th","tr","v","z"];
var nm3b = ["b","br","c","ch","cr","d","dr","g","gn","gr","l","ll","m","n","ph","s","str","th","tr","v","z"];
var nm4 = ["a","e","i","o","u","a","e","i","o","u","a","e","i","o","u","ae","ai","ao","au","a","ea","ei","eo","eu","e","ua","ue","ui","u","ia","ie","iu","io","oa","ou","oi","o"];
var nm5 = ["turn","ter","nus","rus","tania","hiri","hines","gawa","nides","carro","rilia","stea","lia","lea","ria","nov","phus","mia","nerth","wei","ruta","tov","zuno","vis","lara","nia","liv","tera","gantu","yama","tune","ter","nus","cury","bos","pra","thea","nope","tis","clite"];
var nm6 = ["una","ion","iea","iri","illes","ides","agua","olla","inda","eshan","oria","ilia","erth","arth","orth","oth","illon","ichi","ov","arvis","ara","ars","yke","yria","onoe","ippe","osie","one","ore","ade","adus","urn","ypso","ora","iuq","orix","apus","ion","eon","eron","ao","omia"];
var nm7 = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z","0","1","2","3","4","5","6","7","8","9","0","1","2","3","4","5","6","7","8","9","","","","","","","","","","","","","",""];
var br = "";

function nameGen(){
	for(i = 0; i < 10; i++){
		if(i < 2){
			rnd = Math.random() * nm1.length | 0;
			rnd2 = Math.random() * nm2.length | 0;
			rnd3 = Math.random() * nm3.length | 0;
			while(nm1[rnd] === nm3[rnd3]){
			rnd3 = Math.random() * nm3.length | 0;
			}
			rnd4 = Math.random() * nm4.length | 0;
			rnd5 = Math.random() * nm5.length | 0;
			names = nm1[rnd] + nm2[rnd2] + nm3[rnd3] + nm4[rnd4] + nm5[rnd5];
		}else if(i < 4){
			rnd = Math.random() * nm1.length | 0;
			rnd2 = Math.random() * nm2.length | 0;
			rnd3 = Math.random() * nm3.length | 0;
			while(nm1[rnd] === nm3[rnd3]){
			rnd3 = Math.random() * nm3.length | 0;
			}
			rnd4 = Math.random() * nm6.length | 0;
			names = nm1[rnd] + nm2[rnd2] + nm3[rnd3] + nm6[rnd4];
		}else if(i < 6){
			rnd = Math.random() * nm1.length | 0;
			rnd4 = Math.random() * nm4.length | 0;
			rnd5 = Math.random() * nm5.length | 0;
			names = nm1[rnd] + nm4[rnd4] + nm5[rnd5];
		}else if(i < 8){
			rnd = Math.random() * nm1.length | 0;
			rnd2 = Math.random() * nm2.length | 0;
			rnd3 = Math.random() * nm3b.length | 0;
			while(nm1[rnd] === nm3b[rnd3]){
			rnd3 = Math.random() * nm3b.length | 0;
			}
			rnd4 = Math.random() * nm2.length | 0;
			rnd5 = Math.random() * nm5.length | 0;
			names = nm3b[rnd3] + nm2[rnd2] + nm1[rnd] + nm2[rnd4] + nm5[rnd5];
		}else{
			rnd = Math.random() * nm3b.length | 0;
			rnd2 = Math.random() * nm6.length | 0;
			rnd3 = Math.random() * nm7.length | 0;
			rnd4 = Math.random() * nm7.length | 0;
			rnd5 = Math.random() * nm7.length | 0;
			rnd6 = Math.random() * nm7.length | 0;
			names = nm3b[rnd] + nm6[rnd2] + " " + nm7[rnd3] + nm7[rnd4] + nm7[rnd5] + nm7[rnd6];
		}
    }
    return names;
}

module.exports = nameGen()