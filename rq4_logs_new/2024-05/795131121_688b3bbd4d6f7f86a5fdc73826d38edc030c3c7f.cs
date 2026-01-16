using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace FilaDinamica
{
    internal class FilaPessoa
    {
        Pessoa? head;  // Recebe uma pessoa da fila.
        Pessoa? tail; // Recebe uma pessoa da fila.

        public FilaPessoa()
        {
            this.head = null;
            this.tail = null;
        }

        public void Push(Pessoa aux) //Método inserir recebe uma pessoa
        {
            if (IsEmpty())
            {
                //p1 = p1 = p1
                head = tail = aux; // Tail recebe o aux, head recebe o tail
                                   // Todo mundo aponta para o primeiro objeto inserido
            }
            else
            {
                //this.p2.setNext(p3) ----> this.p2.proximo = p3 | agora o atributo proximo de p2 é p3
                //this.p1.setNext(p2) ----> this.p1.proximo = p2 | agora o atributo proximo do objeto p1 é o p2
                this.tail.setNext(aux); // Envia a pessoa recebida pelo parâmetro para o método tail.setNex(aux) - |CLASSE - Pessoa|
                tail = aux; // Agora tail, recebe a pessoa do parâmetro aux. A´pessoa que estava no aux agora é a calda/ultimo elemento inserido/fim da fila
            }

        }

        public void Pop()
        {
            if (!IsEmpty())
            {
                if (tail == head) //verifico se tenho um elemento só
                {
                    head = tail = null;
                }

                else
                {
                    this.head = head.getNext();
                }
            }
        }

        public bool IsEmpty()
        {
            return head == null && tail == null;
            // Retorna se head e tail estão vazias    
        }

        public void Print()
        {
            Pessoa auxPessoa = head;

            if (IsEmpty())
            {
                Console.WriteLine("\n\nFila Vazia!");
                Console.Write("Pressione qualquer tecla para continuar:");
                Console.ReadKey();
            }

            else
            {
                do
                {
                    Console.WriteLine(auxPessoa.ToString());
                    auxPessoa = auxPessoa.getNext();
                    //P1 = P1.seuPróximo.() - retorna o valor P2 pois ele é o valor do atributo |proximo| do objeto P1
                    //P2 = P2.seuPróximo.() - retorna o valor P3 pois ele é o valor do atributo |proximo| do objeto P2


                } while (auxPessoa != null);

                Console.WriteLine("\n FIM DA IMPRESSÃO");
                Console.Write("\nPressione qualquer tecla para continuar: ");
                Console.ReadKey();
            }
        
        }


    }



}