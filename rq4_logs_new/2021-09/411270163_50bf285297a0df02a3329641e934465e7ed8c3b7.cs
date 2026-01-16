using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinkList
{

    //Creamos una clase para el nodo
    internal class LDENode
    {
        //Creamos la estructura que va a contener los nodos
        //Dato del nodo entero
        internal int data;

        //Nodo siguiente=nulo 
        internal LDENode next;

        //Nodo atras
        internal LDENode back;

        //Declaramos metodo get(obtener) retorna datos, metodo set(colocar) datos=valor
        public int Data
        {
            get { return data; }
            set { data = value; }
        }

        //Declaramos metodo get(obtener) retorna nodo siguiente, metodo set(colocar) siguiente=valor 
        public LDENode Next
        {
            get { return next; }
            set { next = value; }
        }
        //Encapsula 
        //Declaramos metodo get(obtener) retorna nodo atras, metodo set(colocar) atras=valor
        public LDENode Back
        {
            get { return back; }
            set { back = value; }
        }
        //Se crea un constructor para indicarle el valor de las variables data = entero  y next= null
        //Constructor
        public LDENode(int d)
        {
            data = d;
            next = null;
        }
    }
    //Creamos una clase para lista doblemente enlazada 
    //Que significa que sea una Lista de Doblemente enlazadas?, 
    //Significa que puede recorrer los valores desde el primer nodo hasta el ultimo y diseversa (doble) del ultimo hasta el primero
    // 3, 5, 6, 7, 9, 1, 12, 34, 25, 30
    internal class DoublyLinkedList
    {

        //Variable de la cabeza (Primer valor de la lista)
        //private LDENode head = new LDENode();
        internal LDENode head;

        //Variable ultima(Ultimo valor de la lista)
        internal LDENode last;

        //Variable nodo
        internal int n;

        internal void Lista()
        {
            head = null;
            last = null;
        }
        //Lista Doblemente Enlazada ------Creacion de Nodo nuevo
        internal DoublyLinkedList()
        {
           // indicarle que el numero de la lista empieza en 0
            this.n = 1;
        }

        //Insertamos nodos a una lista
        //Creamos una inserción del nuevo nodo de enfrente y al final concatenamos para que pueda leer toda la lista 
        internal void InsertFront(int new_data)
        {
            //Declaramos nuevo nodo
            LDENode newNode = new LDENode(new_data);
            //Nodo Ejemplo=3
            //Declaramos a la consola que me introduzca nuevo nodo
            Console.Write("\n Ingrese el dato del nuevo nodo: ");
            //Convertimos valor introducido a la consola a valor entero
            newNode.Data = int.Parse(Console.ReadLine());
            //Porque indicamos que la lista no a sido creada por lo tanto no contiene ningun valor
            if(this.head==null)
            {
                // 3, 5, 6, 7, 9, 1, 12, 34, 25, 30
                //Lista doblemente enlazada 
                //null <------3-------> null

                //Indicamos que el primer nodo de la lista este nuevo
                //Apuntador indicando que Primero=null y eso significa que va apuntar al nodo que le indiquemos en este caso es 3
                //Primero=3
                head = newNode;
                //el nodo 3 contiene un apuntador a 3, es el siguiente dato que va imprimir o aparecer
               //Apunta al siguiente
                //3 apunta a null
                head.Next = null;
                //Apuntador hacia atras
                //Contiene el apuntador hacia atras, indica que tampoco tiene nada por detras
                head.Back = null;
                //Indicar que tambien el ultimo puede ser el primero
                //El ultimo siempre va hacer el primero
                //Primero =3 ultimo=3
                //el ultimo va hacer el primero osea 3
                last = head;
            }
            else
            //El segundo nodo es 5
            {
                // 3(el ultimo nodo) va apuntar al nuevo nodo sea 5
                last.Next = newNode;
                //tmb ese nuevo nodo tiene un apuntar a siguiente
                //null <--- 3 --> 5---> null
                newNode.Next = null;
                //tmb contiene un apuntador hacia atras
                //el nuevo nodo va apuntar al ultimo
                //null <--- 3 --> <----5---> null
                newNode.Back = last;
                //Indicamos ahora que el ultimo es el nuevo nodo
                // y cual es el nuevo nodo el 5
                //null < ---3-- > < ----5---> null (6) Ahora el ultimo va hacer el nuevo nodo que metamos
                //null < ---3-- > < ----5---> 6 --> null (igual tiene un apuntador a siguiente que es null)
                //null < ---3-- > < ----5--->  <---6 --> null (igual tiene un apuntador que apunta al siguiente)
                //y ahora el ultimo valor de nuestra lista es el nuevo nodo =6
                //Primero=3 ultimo= ya no es 3 ahora es 5
                last = newNode;
                n++;
            }
            Console.Write("\n Nuevo nodo ingresado con exito \n"); 
            /*    
            newNode.next = this.head;
            this.head = newNode;
         */

        }

        //Desplegar lista doble
        //Creamos un nuevo nodo que se llama actual (present)
        //Metodo para recorer desde el primero hasta el ultimo
        internal void displaylistPU(int new_present)
        {
            //Declaramos nuevo nodo
            //vamos desde el primer nodo recorriendo la lista
            //1. Creamos un nuevo apuntador llamado actual que hace referencia al primer nodo de la lista el 3
            LDENode present = new LDENode(new_present);
            present = head;
            //mientras actual sea diferente de null, me recorre la lista
            //2.Actual es diferente de 3 si, entonces imprimimos un dato 
            while(present !=null)
            {
                //Los nodos datos que vayas encontrandolos imprimelos
                Console.WriteLine(" " + present.Data);
                //y el nodo recorre la lista con el apuntador Next
                // y ahora el actual no es 3 si  no al siguiente al 5 y asi con todos los valores, eres diferente de null si entonces recorre la lista
                present = present.Next;
                // se termina la lista cuando al final no haiga mas valores que null
            }

        }
        //Desplegar la lista desde el ultimo hacia el primero
        internal void displaylistUP(int new_present)
        {
            LDENode present = new LDENode(new_present);
            //Empiece desde el ultimo
            present = last;
            //Nos permite recorrer la lista mientras actual sea diferente de null
            while(present!=null)
            {
                //Mientras vaya recorriendo los nodos, que nos imprima que va encontrando
                Console.WriteLine(" " + present.Data);
                //usamos apuntar atras es el valor que esta destras de el en este caso recorre la lista hacia atras siendo ser 3
                //ultimo hasta el primero
                present = present.Back;
                //hasta que termina la lista en null
            }
        }


        //Obtener el ultimo nodo
        //Recurre lista a valor nulo
        //Aqui hacemos referencia para indicarle al nodo de la cabeza(el primero), que antes de el llevara null, para indicarle que antes de el no hay nada, le indicamos que desde ahí comienza la lista como tal

        internal LDENode getLastNode()
        {
            LDENode tmp = this.head;
            if (this.isEmpty())
                return null;
            while (tmp.next != null)
            {
                tmp = tmp.next;
            }

            return tmp;
        }

        //Aqui hacemos la insercion del nodo con referencia a nulo. y al final concatenamos para que pueda leer toda la lista
        internal void InsertLast(int new_data)
        {
            LDENode newNode = new LDENode(new_data);
            //Aqui creamos un if para llamar a un metodo que hace referencia al valor nulo IsEmpty
            if (isEmpty())
            //if(head==null)
            {
                this.head = newNode;
                n++;
                return;

            }

            LDENode lastNode = this.getLastNode();
            lastNode.next = newNode;

            n++;
        }
        //Metodo que recorre la lista a valor nudo 
        internal bool isEmpty()
        {
            return head == null;
        }
        //Aqui creamos un contador para que cuente los numeros insertados
        internal int count()
        {
            return n;
        }
        //aqui creamos un metodo para eliminar el nodo que le indicamos a cada lista 
        internal void deleteItem(int val)
        {
            Console.Write("\n Favor de ingresar el nodo que deseamos eliminar: ");
            //Convertimos valor introducido a la consola a valor entero
            val = int.Parse(Console.ReadLine());

            if (!isEmpty())
            {
                if (head.data == val)
                {
                    head = head.next;
                    n--;
                }
                else
                {

                    LDENode prev = this.head;
                    LDENode curr;
                    while (prev.next != null)
                    {
                        curr = prev.next;
                        if (curr.data == val)
                        {
                            prev.next = curr.next;
                            n--;
                            return;
                        }
                        prev = curr;
                    }
                }
            }
        }
        //Aqui imprimimos(Desplegamos) la lista de los datos de nuestra clase
        internal void printList()
        {
            LDENode tmp = this.head;
            if (tmp != null)
                Console.WriteLine(tmp.data);
            while (tmp.next != null)
            {
                tmp = tmp.next;
                Console.WriteLine(tmp.data);
            }
        }
    }





//Aqui le indicamos a la clase main que nos haga un recorrido de lista de los nodos y mandamos a llamar el metodo InsertLast para insertar el nodo aleatorio(random) de la lista
 class Program
    {
        static void Main(string[] args)
    {
        DoublyLinkedList myList = new DoublyLinkedList();

        Random r = new Random();
        //int[] lala = { 3, 5, 6, 7, 9, 1, 12, 34, 25, 30 };
        for (int i = 0; i < 10; i++)
        {
                //myList.InsertFront(r.Next(0, 30));
                // myList.InsertLast(lala[i]);
                myList.InsertFront(r.Next(0, 10));
            }

        //Indicamos cabeza principal
        LDENode tmp = myList.head;
        if (tmp != null)
            Console.WriteLine(tmp.data);
        while (tmp.next != null)
        {
            tmp = tmp.next;
            Console.WriteLine(tmp.data);
        }


            //Aqui desplegamos las listas de los nodos, y las eliminaciones de los nudos asi como imprimimos el contados de los nodos
            Console.WriteLine("\n PRIMERO AL ULTIMO");
            myList.displaylistPU(10);
            Console.WriteLine("LA LISTA TIENE " + myList.count() + " ELEMENTOS");

       
            Console.WriteLine("\n ULTIMO AL PRIMERO");
            myList.displaylistUP(10);
            Console.WriteLine("LA LISTA TIENE " + myList.count() + " ELEMENTOS");


            myList.deleteItem(10);
            Console.WriteLine("\n ELEMENTO ELIMINADO " );
            myList.printList();
            /* 

             myList.deleteItem(3);
             Console.WriteLine("Borramos el 3");
             myList.printList();

             myList.deleteItem(9);
             Console.WriteLine("Borramos el 9");

             myList.printList();


         */

            Console.WriteLine("LA LISTA TIENE " + myList.count() + " ELEMENTOS");
            Console.ReadLine();

        }
      }
    }