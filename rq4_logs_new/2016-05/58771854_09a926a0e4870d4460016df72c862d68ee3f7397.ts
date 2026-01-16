/*
 Написать функцию getUnique(arr), которая принимает аргументом неограниченое число аргументов,
 и возвращает массив уникальных элементов. Аргумент не должен изменяться.
 Порядок элементов результирующего массива должен совпадать с порядком,
 в котором они встречаются в оригинальной структуре.
 Специально обрабатывать значение NaN не обязательно.
 */
export function getUnique(...list:any[]):any[] {
    return list.reduce((out:any[], current:any) => out.indexOf(current) < 0 ? out.concat([current]) : out, []);
}

console.log(JSON.stringify(getUnique(1, 2, 3, 1, 2)) === JSON.stringify([1,2,3]));
console.log(JSON.stringify(getUnique(1, 2, 3)) === JSON.stringify([1,2,3]));