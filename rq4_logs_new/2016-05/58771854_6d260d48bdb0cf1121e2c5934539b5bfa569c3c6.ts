/**
 * Created by igor on 1/9/16.
 */

/**
 * Написать функцию isInArray(), которая начиная со второго принимает переменное количество аргументов.
 * Возвращает true, если все аргументы, кроме первого входят в первый.
 * Первым всегда должен быть массив.
 */
type a= string|number|boolean;
function isInArray<T extends a>(arr:T[], ...elements:T[]):boolean {
    let inArr = true;
    for (let el of elements) {
        if (arr.indexOf(el) === -1) {
            inArr = false;
            break;
        }
    }
    return inArr;
}
console.log(isInArray<number>([1, 4, 5, 6], 3, 5, 6, 7));
console.log(isInArray<boolean|number>([true, false, 1], 2, true));

/**
 * Написать функцию summator(), которая сумирует переданые ей аргументы.
 * Аргументы могут быть либо строкового либо числового типа. Количество их не ограничено
 */
function summator(...elements:number[]):number;
function summator(...elements:string[]):number;
function summator(...elements:any[]):number {
    let sum = 0;
    for (let el of elements) {
        let parseEl = parseFloat(el);
        if (isNaN(parseEl)) {
            sum += 0;
            continue;
        }
        sum += parseEl;
    }
    return sum;
};

console.log(summator(1, 4, 5.5));
console.log(summator('1.2', 'asdasd', '2.2'));


/**
 * Написать функцию getUnique(arr), которая принимает аргументом неограниченое число аргументов,
 * и возвращает массив уникальных элементов. Аргумент не должен изменяться.
 * Порядок элементов результирующего массива должен совпадать с порядком,
 * в котором они встречаются в оригинальной структуре.
 * Специально обрабатывать значение NaN не обязательно.
 */

type snb = string|number|boolean
function getUnique<T>(...args:T[]):T[] {
    let uniqArr:T[] = [];
    for (let arg of args) {
        if (uniqArr.indexOf(arg) !== -1) {
            continue;
        }
        uniqArr.push(arg);
    }
    return uniqArr;
}

console.log( getUnique<snb>(1, 'a', 1, 2, 'a', 'b', true, true, 2));


/** revertSentence (_**/

function revertSentence(sentence:string):string {
    let wordsArr = sentence.replace(/\d*/g, '').split(' ');
    let revertLettersArr = wordsArr.map((word:string):string => {
        return word ? word.replace(/\W*/g, '').split('').reverse().join('') : word;
    }).join('').split('');
    let j = 0;
    return Array.prototype.map.call(sentence, (w:string):string => {
        if (!isNaN(parseInt(w, 10)) || /\W/.test(w)) {
            return w;
        }
        j++;
        return revertLettersArr[j - 1];
    }).join('');
}

let s1 = 's1tar3t 2 hellow';
let s2 = 's1ta$%r3t 2 hel^low';
let s3 = 's1tar3t 2   low5';
console.log(s1, ' -> ', revertSentence(s1));
console.log(s2, ' -> ', revertSentence(s2));
console.log(s3, ' -> ', revertSentence(s3));