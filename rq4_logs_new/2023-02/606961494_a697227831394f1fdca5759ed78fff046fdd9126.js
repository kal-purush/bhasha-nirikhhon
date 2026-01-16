//Ejercicio 1
//Sumar si el primer digito es mayor
class Ejercicio1{
    variables(){
        let num1=3
        let num2=4
        let suma=0
        let ops="+"
        if(num1>num2){
            suma = num1+num2
            console.log(num1,ops,num2,"=",suma)
        } else{
            if(num1<num2)
            console.log("El primer digito no es mayor al segundo")
        }
    }
}
//let ejercicios1 = new Ejercicio1()
//ejercicios1.variables()


//Ejercicio 2
//Realizar una operacion segun el operador
class Ejercicio2{
    variables2(){
        let num1=5,num2=2,num3=5,num4=4,num5=7,num6=2,num7=4,num8=2,num9=10,num10=2
        let suma=0,resta=0,multi=0,divi=0,reciduo=0
        let ops="+",opr="-",opm="*",opd="/",opre="%"
        if(ops){
            suma=num1+num2
            console.log(`Operador suma = ${num1} + ${num2} = ${suma}`)
        }
        if(opr){
            resta=num3-num4
            console.log(`Operador resta = ${num3} - ${num4} = ${resta}`)
        }
        if(opm){
            multi=num5*num6
            console.log(`Operador multiplicacion = ${num5} * ${num6} = ${multi}`)
        }
        if(opd){
            divi=num7/num8
            console.log(`Operador division = ${num7} / ${num8} = ${divi}`)
        }
        if(opre){
            reciduo=num1%num2
            console.log(`Operador reciduo = ${num9} % ${num10} = ${reciduo}`)
        }
    }
}
//let ejercicios2 = new Ejercicio2()
//ejercicios2.variables2()


//Ejercicio 3
//Presentar el mayor de dos numeros

class ejercicio3{
    variables3(){
        let num1=6,num2=8
        if(num1 > num2 ){
            console.log(`${num1} > ${num2} el numero mayor es = ${num1}`) 
        } else{
            if (num2 > num1){
                console.log(`${num2} > ${num1} el numero mayor es = ${num2} `)
            }
        }
    }
}
//let ejercicios3 = new ejercicio3()
//ejercicios3.variables3()

//Ejercicio 4
//Dado un nombre presentarlo
class ejercicio4{
    variables4(){
        let nombre="Samantha"
        console.log(`${nombre}`)
    }
}
//ejercicios4 = new ejercicio4()
//ejercicios4.variables4()


//Ejercicio 5
//Dado el valornde compra presentar el total con un 12% de iva
// 5) Dado un valor de compra presentar el total considerando un 12% de iva
class Ejercicio5{
    variables5(){
      let valor1=70
      let valor2=100
      let valortotal=valor1+valor2
      let valoresconiva=valortotal*1.12
      console.log("")
      console.log("Dado un valor de compra presentar el total considerando un 12% de iva")
      console.log("Valor 1 =",valor1)
      console.log("Valor 2 =",valor2)
      console.log("Valor total =",valortotal)
      console.log("Valor total con iva =",valoresconiva)
    }

}
//ejercicios5 = new Ejercicio5()
//ejercicios5.variables5()




//Ejercicio 6
//Presentar 5 veces un nombre
class Ejercicio6{
    variables6(){
        let nombre="Alexa"
        let c=1
        for(c=1;c<=5;c++){
            console.log(`${c} ${nombre}`)
        }
    }
}
//ejercicios6 = new Ejercicio6()
//ejercicios6.variables6()

//7) Presentar los numeros multiplos de 3 del 3 al 21
class Ejercicio7{
    variables7(){
        let c=1,mul=21
        while(c<=mul){
            if(c % 3 == 0){
                console.log(`${c}`)
            }
            c=c+1
        }
    }
}
//let ejercicios7 = new Ejercicio7()
//ejercicios7.variables7()

//Ejercicio 8
//8) Presentar los numeros multiplos de 3 del 21 al 3
class Ejercicio8{
    variables8(){
        let c=21
        for(c = 21; c>=3 ; c=c-3){
            if(c%3 == 0){
                console.log(`${c}`)
            }
        }
    }
}
//let ejercicios8 = new Ejercicio8()
//ejercicios8.variables8()


//ejercicio 9
//9) Dados dos nombres indicar si son iguales o diferentes
class Ejercicio9{
    arreglo9(){
        let nom1="Samy"
        let nom2="Samy"
        if(nom1 == nom2 ){
            console.log(`${nom1} y ${nom2} son iguales`)
        } else{
            if(nom1 !== nom2 ){
                console.log(`${nom1} no son iguales ${nom2}`)
            }
        }
    }
}
let ejercicios9 = new Ejercicio9()
ejercicios9.arreglo9()


//ejercicio10
//10) Dado diez numeros en un arreglo presentar los que tengan menos de 5 caracteres
class ejercicio10{
    variables10(){
        console.log("Presentar elementos de un arreglo que tengas menos de 5 ")
        let arreglo=["12345","345678","4","34","56","2467","64432","234234","234","324234"]
        let elemento
        for(let pos=0;pos<10;pos=pos+1){
            if(arreglo[pos].length<5){
                console.log(`pos-> ${pos} Elemento:${arreglo[pos]}`)
            }

        }

    }
}
//let ejercicios10 = new ejercicio10()
//ejercicios10.variables10()

//Ejercicio 11
//11) Dado un arreglo presentar sus elementos
class Ejercicio11{
    arreglo11(){
        let elementos = [78,3,4,5,6]
        console.log(`${elementos}`)
    }
}
//let ejercicios11 = new Ejercicio11()
//ejercicios11.arreglo11()


//Ejercicio 12
//12) Dado un arreglo de numeros presentar los menores a 10
class Ejercicio12{
    arreglo12(){
        let elementos = [78,3,4,56,6]
        let longitud = elementos.length,c=0
        console.log(elementos,elementos[0],elementos[longitud-1])
        while(c<longitud){
            if(elementos[c]<10){
                console.log(`${elementos[c]}`)
            }
            c=c+1

        }

    }
}
//let ejercicios12 = new Ejercicio12()
//ejercicios12.arreglo12()

//Ejercicio13
//13) Dado un arreglo de numeros presentar los impares y al final la suma de los pares
class Ejercicio13{
    arreglo13(){
        let numeros=[3,5,6,8,9]
        let longitud = numeros.length,c=0,suma=0
        console.log(numeros,numeros[0],numeros[longitud-1])
        while(c<longitud){
            if(numeros[c] % 2 !==0){
                console.log(`${numeros[c]} es impar`)
            } else{
                suma=suma+numeros[c]
            }
            c=c+1
        }
        console.log(`la suma de los pares es: ${suma}`)
    }
}
//let ejercicios13 = new Ejercicio13()
//ejercicios13.arreglo13()


//Ejercicio14
//14) Presentar el primero el medio y el ultimo valor de un arreglo
class Ejercicio14{
    arreglo14(){
        let numeros=[2,5,7,8,9]
        let longitud = numeros.length,c=0,mult=1
        console.log(numeros,"El primer numero es: ",numeros[0], " El numero de en medio es: ",numeros[longitud-3], " El numero final es: ",numeros[longitud-1])
    }
}
//let ejercicios14 = new Ejercicio14()
//ejercicios14.arreglo14()

// Ejercicio15
//15) Calcular el vuelto de un a compra dado el costo y el pago
class Ejercicio15{
    variables15(){
        console.log("15) Calcular el vuelto de un a compra dado el costo y el pago")
        let costo = 50
        let pago = 30
        let vuelto= 50 - 30
        console.log("el costo es:",costo,"$")
        console.log("el pago es de:",pago,"$")
        console.log("el vuelto es:",vuelto,"$")
}
}
//let ejercicios15 = new Ejercicio15()
//ejercicios15.variables15()



// Ejercicio 16
//Presentar la tabla de multiplicar de cualquier numero del 1 al 12
class Ejercicio16{
    ciclo16(){
        console.log("Presentar la tabla de multiplicar de cualquier numero del 1 al 12")
        console.log("la tabla del 2")
        let n=2
        let n1=1,mul=1
        while(n1<=12){
            mul=n*n1
            console.log(n,"*",n1,"=",mul)
            n1=n1+1
        }
        
    }
}
//let ejercicios16 = new Ejercicio16()
//ejercicios16.ciclo16()



//Ejercicio17
//17) Realizar la multiplicacion de dos numeros por medio de sumas sucesivas
class ejercicio17{
    variables17(){
        let n1=Math.floor(Math.random()*10)
        let n2=Math.floor(Math.random()*10)
        let ac=0,c=0
        while(c<n2){
            ac=ac+n1
            c=c+1
        }
        console.log(`${n1} * ${n2} = ${ac}`)
    }
}
//ejercicios17 = new ejercicio17()
//ejercicios17.variables17()


//Ejercicio 18
//18)Realizar la division de dos numeros por medio de restas sucesivas
class Ejercicio18{
    ciclo18(){
        console.log("Realizar la division de dos numeros por medio de restas sucesivas")
        let n1=10,n2=2
        let c=0
         let res=n1
         while (res-n2>=0){
            res=res-n2
            console.log(res+n2,"-",n2,"=",res)
            c=c+1
         }
        console.log("la division entre:",n1,"/",n2,"=",c)
    }
}
//let ejercicios18 = new Ejercicio18()
//ejercicios18.ciclo18()

//Ejercicio19
//Calcular el factrial de un numero
class Ejercicio19{
    ciclo19(){
        let n=3
        let c=1,fac=1
        while(c<=n){
            fac=fac*c
            c=c+1
        }
        console.log(`El factorial del numero 3 es :${fac}`)
    }
}
//let ejercicios19 = new Ejercicio19()
//ejercicios19.ciclo19()

//Ejercicio20
//20)Calcular el exponente de un numero
class Ejercicio20{
    ciclo20(){
        let n=5, expo=3, po=1,c=0
        let elevado=Math.pow(n,expo)
        console.log("5","^",expo,"=",elevado)
    }
}

//ejercicios20 = new Ejercicio20()
//ejercicios20.ciclo20()


//ejercicio 21 
//Serie fibonacci

class ejercicio21{
    variables21(){
        let a=0,b=1,c=1,cont=3,n=8
        console.log(a,b,c)
        while(cont<n){
            a=b
            b=c
            c=a+b
            console.log(c)
            cont=cont+1
        }
    }
}
//let ejercicios21 = new ejercicio21()
//ejercicios21.variables21()


//Ejercicio 22 invertir numero
class ejercicio22{
    variables22(){
        let num=321564
        let ninvertido=0,d=0
        console.log(num)
        while (num != 0){
            d=num%10
            ninvertido=ninvertido*10+d
            num=Math.trunc(num/10)
        }
        console.log(ninvertido)
    }
}
//let ejercicios22 = new ejercicio22()
//ejercicios22.variables22()


//Ejercicio 23
//23) Presentar los divisores de un numero
class Ejercicio23{
    ciclo23(){
        let num=6
        let c=1
        for(c=1;c<=num;c++){
           if(num% c==0){
              console.log("los divisores son",c)
           }
        }
    }
}
//let ejercicios23 = new Ejercicio23()
//ejercicios23.ciclo23()

//Ejercicio 24

class Ejercicio24{
    ciclo24(){
        console.log("Un numero es perfecto o no")
        let divisores=0,num=8,c=0,sumaperf=0
        for(c=1;c<num;c=c+1){
            if(num % c == 0){
                sumaperf=sumaperf+c
            }
        }
        if(sumaperf==num){
            console.log(num,"Es un numero perfecto")
        }else{
            console.log(num,"No es perfecto")
        }
    }
}
//let ejercicios24 = new Ejercicio24()
//ejercicios24.ciclo24()

//Ejercicio25
//25) Verfificar si un numero es primo o no(Primo cuando solo es divsivible 
//para 1 y el propio numero, es decir no tenga divisores)

class Ejercicio25{
    ciclo25(){
        let num=55
        let c=0
        let primo = true
        for(c = 2; c < num; c++){
            if(num%c == 0){
                primo=false

            }

        }
        if(primo){
            console.log(`El numero ${num} es primo`)
        }else{
            console.log(`El numero ${num} no es primo`)
        }
    }
}
//let ejercicios25 = new Ejercicio25()
//ejercicios25.ciclo25()