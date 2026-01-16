//Valdría hacer let nameVar = X

import { create } from "domain"

//Boolean
let muted: boolean = true
muted = false

//Números
let numerador: number = 42
let denominador: number = 6
let resultado = numerador / denominador

//String 
let nombre: string = 'Carlos'
let saludo= `Me llamo ${nombre}`

//Arreglos
let people: string[] = []
people = ["Isabel", "Nicole","Raul"]
//people.push("Juanlu")

let peopleAndNumbers: Array< string | number > = []
peopleAndNumbers.push("Ricardo")
peopleAndNumbers.push(430)

//Enum
// enum Color{
//     Rojo,
//     Verde,
//     Azul
// }

// let colorFavorito: Color = Color.Verde
// console.log(`Mi color favorito es ${colorFavorito}`)
//Si imprimimos por pantalla nos va a salir: "Mi color favorito es 1", ya que TypeScript con enum devuelve la posición que ocupa en la lista
//Para evitar esto, debemos hacer lo siguiente

enum Color{
    Rojo = "Rojo",
    Verde = "Verde",
    Azul = "Azul"
}

let colorFavorito: Color = Color.Verde
console.log(`Mi color favorito es ${colorFavorito}`)

//Any: Se usa cuando no tenemos la certeza de que tipo va a ser una variable, poniendo any nos quita el error que se hubiera producido
let comodin:any = "Joker"
comodin = {type: "Wildcard"}

//Object
let someObject: object = {type: "Wildcard"}











//Funciones
function add(a: number ,b: number): number{
    return a + b
}

const sum = add(4,6)


function createAdder(a:number): (number) => number{
    return function (b: number){
        return b + a
    }
}

const addFour = createAdder(4)
const fourPlus6 = addFour(6)

function fullName (firstName: string, lastName?: string): string{       //El signo de interrogación permite que sea opcional el segundo parámetro
    return `${firstName} ${lastName}`
}

const carlos = fullName("Carlos","García")
const guille = fullName('Guille')

// function fullName (firstName: string, lastName: string = 'García'): string{       
//     return `${firstName} ${lastName}`
// }

//Si a una persona solo le pusieramos el nombre, por defecto nos pondría que el apellido es García 










//Interfaces

enum Color2{
    Rojo = "Rojo",
    Verde = "Verde"
} 

interface Rectangulo{
    ancho: number,
    alto: number,
    color?: Color2
}

let rec: Rectangulo = {
    ancho:4,
    alto:6,
    color: Color2.Rojo
}

function area(r: Rectangulo): number{
    return r.alto * r.ancho
}

const areaRect = area(rec)
console.log(areaRect)

rec.toString = function(){
    return this.color ? `Un rectángulo ${this.color}` : `Un rectángulo`
}

console.log(rec.toString())


