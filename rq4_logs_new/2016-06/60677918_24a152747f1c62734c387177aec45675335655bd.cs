using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace Produttore_Consumatore_interfacce
{
    static class Buffer
    {

        static private int buf_value;
        static private int n_in_buffer;
        static private int buff_size;

        static public void Buffer_Size(int bsize){
            buff_size = bsize;
       }
        
        static public int Write(int v) {

             buf_value = v;
            if (n_in_buffer < buff_size)
            {
                n_in_buffer = n_in_buffer + 1;
            }

            return n_in_buffer;
  
        }

        static public void Buffer_is_not_full(){

           Task.Run(() =>
            {
                do
                {

                    if (n_in_buffer < buff_size) break;

                } while (true);
                
            }
            
            );
        }


        static public int Read() {

            if (n_in_buffer >0 ) { 
                n_in_buffer = n_in_buffer - 1;
               }

            return n_in_buffer;          
        }


        static public async Task Buffer_is_not_empty()
        {
           await Task.Run(() =>
            {
                do
                {

                    if (n_in_buffer > 0) break;

                } while (true);


            });
            
        }
        
    }



    interface IProduttore {

        void Produci();

    }

    interface IConsumatore
    {

        void Consuma();

    }

}