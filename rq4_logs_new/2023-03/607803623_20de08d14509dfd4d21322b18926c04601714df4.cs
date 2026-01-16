using Lab.EF.Entities;
using Lab.EF.Logic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace LAB.EF.UI
{
     class Program
    {
        static void Main(string[] args)
        {
            //ProductsLogic productsLogic = new ProductsLogic();

            //foreach(Products product in productsLogic.GetAll())
            //{
            //    Console.WriteLine($"{product.ProductName} - {product.UnitPrice}");
            //}

            //RegionLogic regionLogic = new RegionLogic();

            //foreach(var item in regionLogic.GetAll())
            //{
            //    Console.WriteLine($"{item.RegionID} - {item.RegionDescription}");
            //}

            //regionLogic.Add(new Region
            //{
            //    RegionID = 10,
            //    RegionDescription = "Sarasa"
            //});


            //regionLogic.Delete(10);

            //foreach(var item in regionLogic.GetAll())
            //{
            //    Console.WriteLine($"{item.RegionID} - {item.RegionDescription}");
            //}

            // regionLogic.Update(new Region
            // {
            //     RegionID = 10,
            //     RegionDescription = "Nueva Descripcion"
            // });

            //foreach(var item in regionLogic.GetAll())
            //{
            //    Console.WriteLine($"{item.RegionID} - {item.RegionDescription}");
            //}

            //employeesLogic.Add(new Employees
            //{
            //    LastName = "Zambuto",
            //    FirstName = "Agustina"
            //});

            //foreach (var employees in employeesLogic.GetAll())
            //{
            //    Console.WriteLine($"{employees.FirstName} - {employees.EmployeeID}");
            //}

            //employeesLogic.Delete(12);

            //foreach (var employees in employeesLogic.GetAll())
            //{
            //    Console.WriteLine($"{employees.FirstName} - {employees.EmployeeID}");
            //}

            //employeesLogic.Update(new Employees
            //{
            //    EmployeeID = 10,
            //    FirstName = "Gonzalo"
            //});

            //foreach (var employees in employeesLogic.GetAll())
            //{
            //    Console.WriteLine($"{employees.FirstName} - {employees.EmployeeID}");
            //}

            //CategoriesLogic categoriesLogic = new CategoriesLogic();
            //categoriesLogic.Add(new Categories
            //{
            //    CategoryName = "Prueba"
            //});

            //foreach( var categories in categoriesLogic.GetAll())
            //{
            //    Console.WriteLine(categories.CategoryName);
            //}



            /*Practica final*/
            EmployeesLogic employeesLogic = new EmployeesLogic();
            Console.WriteLine("Lista de Empleados:");
            foreach (var employees in employeesLogic.GetAll())
            {
                Console.WriteLine($"- {employees.FirstName} {employees.LastName} - Numero Id: {employees.EmployeeID}");
            }

            Console.WriteLine("");

            Console.WriteLine("Lista de Categorias:");
            CategoriesLogic categoriesLogic = new CategoriesLogic();

            foreach (var categories in categoriesLogic.GetAll())
            {
                Console.WriteLine($"- Nombre: {categories.CategoryName} - Descripcion: {categories.Description} - Numero Id: {categories.CategoryID}");
            }

            Console.WriteLine("");

            Console.WriteLine("Operaciones sobre lista de Empleados");
                Console.WriteLine("Eliga la operacion que desea realizar:");
                Console.WriteLine("1 - Agregar:");
                Console.WriteLine("2 - Borrar:");
                Console.WriteLine("3 - Modificar:");
            int operacion = Int32.Parse(Console.ReadLine());

            switch (operacion)
            {
                case 1:
                    Console.WriteLine("Ingrese Nombre");
                    string addNombre = Console.ReadLine();
                    Console.WriteLine("Ingrese Apellido");
                    string addApellido = Console.ReadLine();
                    employeesLogic.Add(new Employees
                    {
                        FirstName = addNombre,
                        LastName = addApellido
                    });
                    break;
                case 2:
                    Console.WriteLine("Ingrese ID del Empleado que desea eliminar");
                    int deleteId = Int32.Parse(Console.ReadLine());
                    employeesLogic.Delete(deleteId);
                    break;
                case 3:
                    Console.WriteLine("Ingrese Nombre nuevo");
                    string updateNombre = Console.ReadLine();
                    Console.WriteLine("Ingrese Apellido nuevo");
                    string updateApellido = Console.ReadLine();
                    Console.WriteLine("Ingrese ID del empleado que desea modificar");
                    int updateID = Int32.Parse(Console.ReadLine());
                    employeesLogic.Update
                        (new Employees
                    {
                        FirstName = updateNombre,
                        LastName = updateApellido,
                        EmployeeID = updateID
                    });
                    break;
            }

            if(operacion < 1 || operacion > 3)
            {
                Console.WriteLine("Por favor ingrese un numero de las opciones");
            }







            //Console.WriteLine("Eliga una opcion");
            //Console.WriteLine("1 - Lista Empleados");
            //Console.WriteLine("2 - Lista Categorias");
            //int eleccionLista = Int32.Parse(Console.ReadLine());



            //    if(eleccionLista == 1 )
            //    {
            //    Console.WriteLine("Eliga la operacion que desea realizar:");
            //    Console.WriteLine("1 - Listar:");
            //    Console.WriteLine("2 - Agregar:");
            //    Console.WriteLine("3 - Borrar:");
            //    Console.WriteLine("4 - Modificar:");
            //    int operacion = Int32.Parse(Console.ReadLine());
            //    switch (operacion)
            //    {
            //        case 1:
            //            foreach (var employees in employeesLogic.GetAll())
            //            {
            //                Console.WriteLine($"{employees.FirstName} - {employees.EmployeeID}");
            //            }
            //            break;
            //        case 2:
            //            Console.WriteLine("Eliga Nombre Empleado");
            //            employeesLogic.Add(new Employees
            //            {
            //                FirstName = Console.ReadLine()
            //            });
            //            break;
            //        case 3:
            //            Console.WriteLine("Operacion3");
            //            break;
            //        case 4:
            //            Console.WriteLine("Operacion4");
            //            break;
            //    }


            //}
            //else if(eleccionLista == 2) 
            //    {
            //        Console.WriteLine("Opcion 2");
            //    }
            //    else
            //    {
            //        eleccionLista = 1;
            //        Console.WriteLine("Por defecto le asignamos la opcion 1");
            //        Console.WriteLine($"{eleccionLista}");
            //}





            Console.ReadLine();
        }
        
    }
}