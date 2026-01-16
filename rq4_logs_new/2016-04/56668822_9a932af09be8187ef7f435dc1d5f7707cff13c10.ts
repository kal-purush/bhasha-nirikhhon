// http://www.typescriptlang.org/Handbook#basic-types-array
/* Los tipos de variables son: boolean, number, string, array, enum, any y void.
* Los que no encontramos en JavaScript directamente se explican a continuacion*/
// EJEMPLO DE TIPO CONOCIDO EN JS
var listNum:number[] = [1,2,3];
// TIPOS NUEVOS
// Podemos usar <any> para indicar cualquier tipo
var listAny:any[] = [1, true, "free"];
// ARRAY
// Con una variable normal:
var myAnyType: any = 4;
myAnyType = 'I want a string';
myAnyType = false;
var listNumAlternative:Array<number> = [1,2,3];

// TUPLE
// http://www.typescriptlang.org/docs/handbook/basic-types.html#tuple
/** Al contratio que otros lenguajes, como python, las tuplas pueden cambiar.
 */

// ENUM
// http://www.typescriptlang.org/Handbook#basic-types-enum
/** El enum funciona al contrario que un Array.
En lugar de acceder a "Green" mediante la posicion 1,
accedemos al 1 mediante "Green" */

// let doc: https://basarat.gitbooks.io/typescript/content/docs/let.html
/** let es como var pero actua dentro del bloque de codigo. Ojo compatibilidad.
var i = 0
for (let i = 0; i < 3; i++) {
    // la variable i declarada con let existira solamente dentro del for
}
// En el alert usamos la "i" declarada con var no con let
alert(i); // 0
*/
enum Color {Red, Green, Blue};
let c: Color = Color.Green;
console.log(c);

// Tenemos dos alternativas:
// 1.- Cambiamos la ordenacion.
enum ColorAlternative0 {Red = 1, Green, Blue};
var cAlt0: ColorAlternative0 = ColorAlternative0.Green;
console.log(cAlt0);

// 2.- Accedemos mediante posicion en lugar de su nombre */
enum ColorAlternative1 {Red = 1, Green, Blue};
var colorName: string = ColorAlternative1[2];
console.log(colorName);

/* El tipo void es el contrario a any, no devuelve nada.
* Suele verse en funciones que no tienen return*/
function warnUser(): void {
    alert("This is my warning message");
}