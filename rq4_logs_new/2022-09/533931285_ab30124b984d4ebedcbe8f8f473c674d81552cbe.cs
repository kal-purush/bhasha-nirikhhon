using Microsoft.AspNetCore.Mvc;

namespace todo_backend.Controllers;

[ApiController]
[Route("[controller]")]

public class TodoController : ControllerBase
{
    private readonly ILogger<TodoController> _logger;

    public TodoController(ILogger<TodoController> logger)
    {
        _logger = logger;
    }
    
    [HttpGet(Name = "GetTodoList")]
    public IEnumerable<TodoModel> Get()
    {
        TodoModel a = new TodoModel()
        {
            Index = 0,
            Title = "title",
            Content = "1111",
            Status = TodoStatus.NotFinish
        };
        List<TodoModel> test = new List<TodoModel>();
        test.Add(a);
        return test;
    }
}