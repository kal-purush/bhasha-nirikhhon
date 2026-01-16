using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TodosWebApp.DataAccess.Entities;
using TodosWebApp.DataAccess.Interfaces;

namespace TodosWebApp.BusinessLogic
{
    public class TodoService : ITodoService
    {
        private readonly ITodoRepository _todoRepository;

        public TodoService(ITodoRepository todoRepository)
        {
            _todoRepository = todoRepository;
        }

        public async Task<List<Todo>> GetAllAsync()
        {
            return await _todoRepository.GetAllAsync();
        }
    }
}