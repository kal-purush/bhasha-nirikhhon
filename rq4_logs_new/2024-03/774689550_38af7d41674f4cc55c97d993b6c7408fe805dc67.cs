using DataAccess.CRUD;
using DTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CoreApp
{
    public class AppointmentManager
    {

        public void Create(Appointment appointment) 
        {
            // Se crea una instancia de el CrudFactory para interactuar con la capa de acceso a datos (DAO)
            var aptc = new AppointmentCrudFactory();

            // Aquí se realizan validaciones 
            

            //Aqui se ejecuta el metodo del crud
            aptc.Create(appointment);
        }

        public void Update(Appointment appointment) 
        {
            var aptc = new AppointmentCrudFactory();

            // Aquí se realizan validaciones 


            //Aqui se ejecuta el metodo del crud
            aptc.Update(appointment);
        }

        public void Delete(Appointment appointment)
        {
            var aptc = new AppointmentCrudFactory();

            // Aquí se realizan validaciones 


            //Aqui se ejecuta el metodo del crud
            aptc.Delete(appointment);
        }
    }
}