/**
 *  Написать функцию getUnique(arr), которая принимает аргументом неограниченое число аргументов,
 * и возвращает массив уникальных элементов. Аргумент не должен изменяться.
 * Порядок элементов результирующего массива должен совпадать с порядком,
 * в котором они встречаются в оригинальной структуре.
 * Специально обрабатывать значение NaN не обязательно.
 */

type a=number|string|boolean;
function getUnique<T extends a>(...params:T[]):T[]{
    return params.filter(function(el, index, array){
       return array.indexOf(el) === index;
    });
}

console.log(getUnique<number>(1,2,2,3,2,3));
console.log(getUnique<number|string>(1,2,2,3,'2',3));
console.log(getUnique<number|string|boolean>(1,true,2,3,'true',3,'2'));
