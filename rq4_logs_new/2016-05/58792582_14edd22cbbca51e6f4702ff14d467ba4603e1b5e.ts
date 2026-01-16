/**
 * Created by iiskhakov on 19.05.16.
 */

// 1. /////////////////////////////////////////////////////////
function isInArray(arr : any[] ,...args : any[]) : boolean{
    return args.every( (elem) => arr.indexOf(elem) !== -1);
}

console.log(isInArray([1, 2, 3, "a"], 2, 3, 'a'));

// 2. /////////////////////////////////////////////////////////
function summator(...args : (string | number)[]) : number{
    return  args.reduce<number>((sum, arg) => sum + (isNaN(+arg) ? 0 : +arg) , 0);
}

console.log(summator('1','a', '3a', '3.1', 5, .4)); // 9.5

// 3. /////////////////////////////////////////////////////////
function getUnique(...args : any[]) : any[]{
    return args.filter((elem,index) => index === args.indexOf(elem));
};

let v = {f: '123'};
let v2 = v;
console.log(getUnique(null, `a`,'a', 'b', 'a', 'c', v, 'a', {a: 1}, v2, {a: 1}, undefined));

// 4. /////////////////////////////////////////////////////////
function reverseOneWord(str: string) : string{
    if(str.length === 1)
        return str;
    let f : number = 0,                 // forward indexer
        b : number = str.length - 1;    // backward indexer
    let result: string[] = str.split('');
    while(f<b){
        while(f<b && !result[f].match(/[a-z]/i)) { f++; };
        while(f<b && !result[b].match(/[a-z]/i)) { b--; };
        if(f<b){
            let tmp = result[f];
            result[f] = result[b];
            result[b] = tmp;
            f++;
            b--;
        }
    }
    return result.join('');
}
function reverseWords(str: string) : string{
    return str.split(' ').map(reverseOneWord).join(' ');
}

console.log(reverseWords('s1tar3t 2 hellow'));    //t1rat3s 2 wolleh
console.log(reverseWords('s1ta$%r3t 2 hel^low')); //  ->  t1ra$%t3s 2 wol^leh
console.log(reverseWords('s1tar3t 2   low5 '));   // ->  t1rat3s 2   wol5
