var readline = require('readline-sync')
var a = readline.question('Masukan bilangan bulat: ')
var b = readline.question('Masukan bilangan riil: ')
console.log ("\n")

console.log('Sebelum Konversi Tipe Data')
console.log('Tipe a:' + typeof a)
console.log('Tipe b:' + typeof b)
console.log ("\n")

hasil1= parseInt(a);   
hasil2= parseInt(b);    
console.log('Sesudah Konversi Tipe Data')
console.log('Tipe a:' + typeof hasil1)
console.log('Tipe b:' + typeof hasil2)