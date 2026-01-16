const currentDay = new Date().getDay();

switch(new Date().getDay()){
    case 0:
        console.log("new Date().getDay()===Sunday");
        break;
    case 1:
        console.log('new Date().getDay()===Monday');
        break;
    case 2:
        console.log("new Date().getDay()===Tuesday");
        break;
    case 3:
        console.log("new Date().getDay()===Wednesday");
        break;
    case 4:
        console.log("new Date().getDay()===Thursday");
        break;
    case 5:
        console.log("new Date().getDay()===Friday");
        break;
    case 6:
        console.log("new Date().getDay()===Saturday");
    default:
        console.log('new Date().getDay()===other day');
        break;
}




let myArr = new Array(100);
    for (let i = 0; i < myArr.length; i++) {
    console.log(myArr[i]);
    }



let i=0;
while(i<51){
    console.log(i);
    i++};



let j=0;
do{
    console.log(j);
    j++;
}while(j<151);


let myArr2=[i]
while(myArr2<1001){
    console.log(myArr2)
    myArr2++
} 