using DataAccess.DAOs;
using DTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAccess.CRUD
{
    //Padre abstracto de todos los cruds existentes en la arquitectura 
    //Para que solo cuadno ocupo un metodo especifico se cree con lo que ocupe
    public abstract class CrudFactory
    {
        protected SqlDao _dao; //Para que solo quede entre clases 

        //Contrato de los Cruds
        //Obligan a definir los metodos de Create,Retrieve,Update,Delete

        public abstract void Create(BaseDTO baseDTO); //instacia los parametros le manda lo que este en BaseDto
        public abstract void Update(BaseDTO baseDTO);
        public abstract void Delete(BaseDTO baseDTO);
        public abstract T Retrieve<T>();
        public abstract T RetrieveById<T>(int id);

        public abstract List<T> RetrieveAll<T>();
    }
}