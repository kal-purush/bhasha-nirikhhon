/**
 * Created by constantant on 18.05.2016.
 */

function wordRevers(text:string):string{
    
    return text.replace(/([^\s]+)/g, reversed);

    function reversed(word:string):string{
        let wordLen:number = word.length,
            temp1:string[] = Array(wordLen),
            temp2:string[] = [];
        
        for(let i = 0; i < wordLen; i++){
            let symbol = word[i],
                isLetter = /[a-zA-Z]/.test(symbol);
            
            if(isLetter){
                temp2.push(symbol);
            }
            
            if(!isLetter){
                temp1[i] = (symbol);
            }
        }
        
        for(let i = 0; i < wordLen; i++){
            if(!temp1[i]){
                temp1[i] = temp2.pop();
            }
        }
        
        return temp1.join('');
    }
}


console.log('s1tar3t 2 hellow =>', wordRevers('s1tar3t 2 hellow'));
console.log('s1ta$%r3t 2 hel^low =>', wordRevers('s1ta$%r3t 2 hel^low'));
console.log('s1tar3t 2   low5 =>', wordRevers('s1tar3t 2   low5'));