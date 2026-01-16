 using System;

namespace Tarea_15ejercicios
{
    class Program
    {
        static void Main(string[] args)
        {
            int opcion = 0;
            Console.WriteLine("--------------Menú principal-------------");
            Console.WriteLine("      Ejercicio 1) 4 alumnos ");
            Console.WriteLine("      Ejercicio 2) Ingreso de datos");
            Console.WriteLine("      Ejercicio 3) Suma de precios");
            Console.WriteLine("      Ejercicio 4) Notas prácticas");
            Console.WriteLine("      Ejercicio 5) A/Desa provado");
            Console.WriteLine("      Ejercicio 6) Suma de números");
            Console.WriteLine("      Ejercicio 7) Tipo de triángulo");
            Console.WriteLine("      Ejercicio 8) Nombre de la figura");
            Console.WriteLine("      Ejercicio 9) Estaciones del año");
            Console.WriteLine("      Ejercicio 10) Número menor y mayor");
            Console.WriteLine("      Ejercicio 11) Sueldo mayor");
            Console.WriteLine("      Ejercicio 12) Evaluar números (3)");
            Console.WriteLine("      Ejercicio 13) Sucesión Fibonacci");
            Console.WriteLine("      Ejercicio 14) Factorial");
            Console.WriteLine("                15) Salir");
            Console.WriteLine("   Selccione el ejercicio que desea realizar");
            opcion = int.Parse(Console.ReadLine());


            switch (opcion)
            {
                case 1:
                    {
                        string[] nombres = new string[4];
                        int[] notas = new int[4];

                        Console.Clear();
                        for (int i = 0; i < 4; i++)
                        {
                            Console.WriteLine("Ingrese el nombre de los alumnos");
                            nombres[i]= Console.ReadLine();
                        }
                        for (int i = 0; i < 4; i++)
                        {
                            Console.WriteLine("Ingrese la nota de los alumnos");
                            notas[i] = int.Parse(Console.ReadLine());
                        }
                        for (int i = 0; i < 4; i++)
                        {
                            Console.WriteLine("Nombre del alumno: " + nombres[i]);
                            Console.WriteLine("Nota obtenida: " + notas[i]);
                        }
                        break;
                    }
                case 2:
                    {
                        Console.Clear();
                        string[] clientes = new string[5];
                        int [] edad = new int[5];
                        string[] sexo = new string[5];
                        for (int i = 0; i < 5; i++)
                        {
                            Console.WriteLine("Ingrese el nombre y apellido del cliente");
                            clientes[i] = Console.ReadLine();
                            
                        }
                        for (int i = 0; i < 5; i++)
                        {
                            Console.WriteLine("Ingrese la edad del cliente");
                            edad[i] = int.Parse(Console.ReadLine());
                           
                        }
                        for (int i = 0; i < 5; i++)
                        {
                            Console.WriteLine("Ingrese el sexo del cliente");
                            sexo[i] = Console.ReadLine();
                            
                        }
                        Console.Clear();
                        for (int i = 0; i < 5; i++)
                        {
                            Console.WriteLine("Cliente: " + clientes[i]);
                            Console.WriteLine("Edade: " + edad[i]);
                            Console.WriteLine("Sexo: " + sexo[i]);
                        }


                        break;
                    }
                case 3:
                    {
                        Console.Clear();
                        int pro1 = 0, pro2 = 0, pro3 = 0, suma = 0;
                        Console.WriteLine("Ingrese el precio del primer producto");
                        pro1 = int.Parse(Console.ReadLine());
                        Console.WriteLine("Ingrese el precio del segundo producto");
                        pro2 = int.Parse(Console.ReadLine());
                        Console.WriteLine("Ingrese el precio del tercer producto");
                        pro3 = int.Parse(Console.ReadLine());
                        suma = pro1 + pro2 + pro3;
                        Console.WriteLine("El total de los productos será de: "+suma);

                        break;
                    }
                case 4:
                    {

                        Console.Clear();
                        break;
                    }
                case 5:
                    {
                        Console.Clear();
                        break;
                    }
                case 6:
                    {
                        Console.Clear();
                        int [] numero = new int[10];
                        int suma = 0;
                        for (int i = 0; i < 10; i++)
                        {
                            Console.WriteLine("Ingrese un número");
                            numero[i] = int.Parse(Console.ReadLine());
                           
                           
                        }
                        Console.WriteLine("La suma de los números es: " + suma);
                        break;
                    }
                case 7:
                    {
                        Console.Clear();
                        int lado1 = 0, lado2 = 0, lado3 = 0;
                        Console.WriteLine("Ingrese el primer lado del triángulo");
                        lado1 = int.Parse(Console.ReadLine());
                        Console.WriteLine("Ingrese el segundo lado del triángulo");
                        lado2 = int.Parse(Console.ReadLine());
                        Console.WriteLine("Ingrese el último lado del triángulo");
                        lado3 = int.Parse(Console.ReadLine());
                        if ((lado1==lado2)&&(lado1==lado3)&&(lado2==lado3)&&(lado2==lado1)&&(lado3==lado1)&&(lado3==lado2))
                        {
                            Console.WriteLine("El triángulo es equilatero");
                        }  
                        else if ((lado1!=lado2)&&(lado1!=lado3)&& (lado2 != lado1) && (lado2 != lado3)&& (lado3 != lado1) && (lado3 != lado2))
                        {
                            Console.WriteLine("El triangulo es escaleno");
                        }
                        else
                        {
                            Console.WriteLine("El triángulo es isósceles");
                        }

                        break;
                    }
                case 8:
                    {
                        Console.Clear();
                        int lados = 0;
                        Console.WriteLine("Ingrese el número de lados de la figura");
                        lados = int.Parse(Console.ReadLine());
                        
                            if (lados==3)
                            {
                            int perimetro = 0, b=0, h = 0;
                                Console.WriteLine("La figura es un triángulo");
                            Console.WriteLine("Ingrese el la base");
                            b = int.Parse(Console.ReadLine());
                            Console.WriteLine("Ingrese la altura");
                            h = int.Parse(Console.ReadLine());
                            perimetro = ((b * h) / 2);
                            Console.WriteLine("El perímetro es: " + perimetro);
                            }
                            else if (lados==4)
                            {
                            int perimetro = 0, b = 0, h = 0;
                            Console.WriteLine("La figura es un cuadrado/rectángulo");
                            Console.WriteLine("Ingrese el la base");
                            b = int.Parse(Console.ReadLine());
                            Console.WriteLine("Ingrese la altura");
                            h = int.Parse(Console.ReadLine());
                            perimetro = (b * h);
                            Console.WriteLine("El perímetro es: " + perimetro);
                            }
                            else if (lados == 5)
                            {
                                Console.WriteLine("La figura es un pentagono");
                            int perimetro = 0;
                            int[] medidas = new int[5];
                            for (int i = 0; i < 5; i++)
                            {
                                Console.WriteLine("Ingrese la medida del lado");
                                medidas[i] = int.Parse(Console.ReadLine());
                                perimetro += medidas[i];
                            }
                            Console.WriteLine("El perímetro es: " + perimetro);
                            }
                            else if (lados == 6)
                            {
                                Console.WriteLine("La figura es un hexágono");
                            int perimetro = 0;
                            int[] medidas = new int[6];
                            for (int i = 0; i < 6; i++)
                            {
                                Console.WriteLine("Ingrese la medida del lado");
                                medidas[i] = int.Parse(Console.ReadLine());
                                perimetro += medidas[i];
                            }
                            Console.WriteLine("El perímetro es: " + perimetro);
                        }
                        else if (lados == 7)
                            {
                                Console.WriteLine("La figura es un heptágono");
                            int perimetro = 0;
                            int[] medidas = new int[7];
                            for (int i = 0; i < 7; i++)
                            {
                                Console.WriteLine("Ingrese la medida del lado");
                                medidas[i] = int.Parse(Console.ReadLine());
                                perimetro += medidas[i];
                            }
                            Console.WriteLine("El perímetro es: " + perimetro);
                        }
                            else if (lados == 8)
                            {
                                Console.WriteLine("La figura es un octágono");
                            int perimetro = 0, lado = 0;
                            Console.WriteLine("Ingrese la longitud de un lado");
                            lado = int.Parse(Console.ReadLine());
                            perimetro = lado * 8;
                            Console.WriteLine("El perímetro es: " + perimetro);
                            }
                            else if (lados == 9)
                            {
                                Console.WriteLine("La figura es un eneágono");
                            int perimetro = 0, lado = 0;
                            Console.WriteLine("Ingrese la longitud de un lado");
                            lado = int.Parse(Console.ReadLine());
                            perimetro = lado * 9;
                            Console.WriteLine("El perímetro es: " + perimetro);
                        }
                        else if (lados == 10)
                        {
                            Console.WriteLine("La figura es un decágono");
                            int perimetro = 0, lado = 0;
                            Console.WriteLine("Ingrese la longitud de un lado");
                            lado = int.Parse(Console.ReadLine());
                            perimetro = lado * 10;
                            Console.WriteLine("El perímetro es: " + perimetro);
                        }
                        else
                        {
                            Console.WriteLine("Ingrese los lados correctos de la figura");
                        }


                        break;
                    }
                case 9:
                    {
                        Console.Clear();
                        string[] estaciones = new string[4];
                        for (int i = 0; i < 4; i++)
                        {
                            Console.WriteLine("Ingrese la estación del año");
                            estaciones[i] = Console.ReadLine();
                            Console.WriteLine("Las estaciones del año ingresadas son: " + estaciones[i]);
                        }

                        break;
                    }
                case 10:
                    {
                        Console.Clear();
                        int[] numero = new int[5];
                        for (int i = 0; i < 5; i++)
                        {
                            Console.WriteLine("Ingrese un número");
                            numero[i] = int.Parse(Console.ReadLine());
                        }
                        Array.Sort(numero);
                        Console.WriteLine("El número mayor será: " + numero[4]);
                        Console.WriteLine("El número menor será: " + numero[0]);
                        break;
                    }
                case 11:
                    {
                        Console.Clear();
                        string[] empleado = new string[5];
                        int[] sueldo = new int[5];
                        for (int i = 0; i < 5; i++)
                        {
                            Console.WriteLine("Ingrese el nombre del empleado");
                            empleado[i] = (Console.ReadLine());
                        }
                        for (int i = 0; i < 5; i++)
                        {
                            Console.WriteLine("Ingrese el sueldo del trabajador");
                            sueldo[i] = int.Parse(Console.ReadLine());
                        }
                        Array.Sort(empleado);
                        Array.Sort(sueldo);
                        Console.WriteLine("El mayor sueldo corresponde a: [{0}, siendo este de: {1}]", empleado[4], sueldo[4]);
                        break;
                    }
                case 12:
                    {
                        Console.Clear();
                        int a = 0, b = 0, c = 0;
                        Console.WriteLine("Ingrese el primero número");
                        a = int.Parse(Console.ReadLine());
                        Console.WriteLine("Ingrese el segundo número");
                        b = int.Parse(Console.ReadLine());
                        Console.WriteLine("Ingrese el tercer número");
                        c = int.Parse(Console.ReadLine());
                        if ((a >b)&&(a >c))
                        {
                            Console.WriteLine("El número mayor es: " + a);

                        }
                        else if ((b >a)&&(b >c))
                        {
                            Console.WriteLine("El número mayor es: " + b);
                        }
                        else if ((c > a) && (c > b))
                        {
                            Console.WriteLine("El número mayor es: " + c);
                        }
                        else
                        {
                            Console.WriteLine("Los números son iguales");
                        }

                        if ((a < b) && (a < c))
                        {
                            Console.WriteLine("El número menor es: " + a);
                        }
                        else if ((b <a) &&(b <c))
                        {
                            Console.WriteLine("El número menor es: " + b);
                        }
                        else if ((c < a) && (c < b))
                        {
                            Console.WriteLine("El número menor es: " + c);
                        }
                        else
                        {
                            Console.WriteLine("Los números son iguales");

                        }


                        break;
                    }
                case 13:
                    {
                        Console.Clear();
                        int numero = 0, a = 0, b = 1, c = 0, i = 2;
                        string resultado = "";

                        Console.WriteLine("Ingrese un número");
                        numero = int.Parse(Console.ReadLine());
                        if (numero > 0)
                        {
                            resultado = a.ToString();
                            if (numero >1)
                            {
                                resultado += b.ToString();
                                while(i <numero)
                                {
                                    c = a + b;
                                    resultado += c.ToString();
                                    a = b;
                                    b = c;
                                    i++;
                                }
                                
                            }

                        }
                        Console.Write("La sucesión Fibonacci será: " + resultado);

                        break;
                    }
                case 14:
                    {
                        Console.Clear();
                        int factorial=1, resultado = 1;
                        Console.WriteLine("Ingrese el número para conocer su factorial");
                        factorial = int.Parse(Console.ReadLine());
                        for (int i = 2; i <= factorial; i++)
                        {
                            resultado *= i;
                        }
                        Console.WriteLine("El factorial del número es: " + resultado);
                        break;
                    }
                case 15:
                    {
                        Console.Clear();
                        Environment.Exit(0);
                        break;
                    }
             
                default:
                    {
                        Console.Clear();
                        Console.WriteLine("seleccione una opción válida");  
;                        break;
                    }
            }


            Console.ReadKey();
        }
    }
}