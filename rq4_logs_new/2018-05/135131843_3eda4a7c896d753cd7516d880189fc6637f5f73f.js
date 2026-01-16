var x = ['a',['a','b','b'],'c'];
//console.log(x);
// var tokens= [{
//     access:{
//         type: String,
//         required:true
//     },
//     token:{
//         type:String,
//         required:true
//     }
// }];
// var access = 'auth';
// var token = 'plm';
// tokens.push({access, token});
// console.log(tokens);
var functie = ( a,  b) => {
    return new Promise((resolve,reject) => {
        if(a>b){
            resolve(a+b);
        }else{
            reject('Because you suck dick')
        }
    });
}
functie(1,3).then((rezultat) => {
    return new Promise((resolve, reject) => {
        if(rezultat > 10){
            resolve(rezultat);
        }else{
            reject('Aolo');
        }
    });

}).then((next) => {
    return next;
}).then((rezultat) => {
    console.log(rezultat);
}).catch((e) => {
    console.log(e);
})
var anotherFunction = function(a){
    
}