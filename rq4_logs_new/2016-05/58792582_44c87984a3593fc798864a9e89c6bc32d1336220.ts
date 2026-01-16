/**
 * Created by constantant on 18.05.2016.
 */

function summator(...numbers:(number|string)[]):number{
    let sum = 0;
    numbers.forEach(function (num:number|string, index:number) {
        let value = typeof num === 'string' ? parseFloat(num) : num;
        
        if(isNaN(value) || typeof value !== 'number'){
            throw new TypeError(`Argument #${index} is not valid!`);
        }
        sum += value
    });
    return sum;
}

console.log(
    //summator('33',1,1,{},1),
    summator(1,1,1,1,1),
    summator(1,'1',1,'1',1),
    summator(1,'1.5',1,'0.5',1)
);