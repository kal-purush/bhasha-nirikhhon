using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Wunderlist.Models;

namespace Wunderlist.Service.Interfaces
{
    public interface ICategoryService
    {
        void Add(ToDoItemList entity);
        void Update(ToDoItemList entity);
        void Delete(ToDoItemList entity);
        void Delete(int id);
        ToDoItemList GetById(int id);       
        bool AddUserInList(int id,UserProfile userProfile);
    }
}