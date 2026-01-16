
fodasepacas = document.getElementById('123');

fodase_morse = ['.-','-...','-.-.','-..','.','..-.','--.','....','..','.---','-.-','.-..','--','-.','---','.--.','--.-',
'.-.','...','-','..-','...-','.--','-..-','-.--','--..']
fodase_letra = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']



function fodasemuito(){
    certo = [];
    i = fodasepacas.value;
    y = 0;

    while(y != i){
        rand = Math.floor(25* Math.random());

        x = prompt('Qual é essa letra? '+ fodase_morse[rand]);

        if(x == fodase_letra[rand]){
            alert('Parabuains');
            certo.push(0);
        }else{
            alert('Burro pa caralho o bagulho certo é '+fodase_morse[rand]+' = '+fodase_letra[rand])
        }

        y++;
    }
    p = (certo.length/i) * 100
    alert('Você acertou: '+ certo.length+'/'+i+' ou seja '+ p+'%')
}