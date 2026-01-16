/**
 * Created by constantant on 18.05.2016.
 */

function isInArray(array: any[], ...targets: any[]):boolean {

    if(!Array.isArray(array)){
        throw new TypeError('First argument must be like an Array! ');
    }

    if(targets.length === 0){
        throw new Error('No target!');
    }

    return targets.every(function (target:any) {
        return array.some(function (item:any) {
            return item === target;
        });
    });
}



let arr:any[] = [],
    num1:number = 1,
    num2:string = '2',
    obj1:{a:number} = {a:1},
    obj2:number[] = [1,2,3];

arr.push(num1);
arr.push(num2);
arr.push(obj1);
arr.push(obj2);

console.log(
    isInArray(arr, num1, obj1),
    isInArray(arr, 33, obj2),
    isInArray(arr, obj2, num2, num1),
    isInArray(arr, obj2, '1212', num1)
    //isInArray(arr)
    //isInArray(123, obj2, '1212', num1)
);