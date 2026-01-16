using Microsoft.AspNetCore.Mvc;
using Task_Manager_Application.Mvc.Entities.Repositories;
using Task_Manager_Application.Mvc.Infrastructure.UnitOfWork;

namespace Task_Manager_Application.Mvc.Controllers
{
    public class TasksController : Controller
    {
        private readonly ITaskRepository _taskRepository;
        private readonly IUnitOfWork _unitOfWork;

        public TasksController(ITaskRepository taskRepository, IUnitOfWork unitOfWork)
        {
            _taskRepository = taskRepository;
            _unitOfWork = unitOfWork;
        }

        public async Task<IActionResult> Index()
        {
            var tasks = await _taskRepository.GetAllAsync();
            return View(tasks);
        }
        [HttpGet]
        public async Task<IActionResult> CreateTask()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> CreateTask(Models.Task task)
        {
            var createTask = new Models.Task()
            {
                Name = task.Name,
                Description = task.Description,
                DateCompleted = task.DateCompleted,
                DateCreated = DateTime.Now,
                DateModified = DateTime.Now,
                PersonNames = task.PersonNames,
                Priority = task.Priority,
                Status = Enums.StatusEnum.To_Do,
            };

            await _taskRepository.AddAsync(createTask);
            await _unitOfWork.CommitAsync();
            return View("Response", createTask);

        }
        [HttpGet]
        public async Task<IActionResult> Response()
        {
            return View();
        }

        [HttpGet]
        public async Task<IActionResult> EditTask(int id)
        {
            var getTask = await _taskRepository.GetAsync(x => x.Id == id);
            if (getTask != null)
            {
                return View("EditTask", getTask);
            }

            return RedirectToAction("Index");
        }
        [HttpPost]
        public async Task<IActionResult> UpdateTask(Models.Task task)
        {
            await _taskRepository.UpdateAsync(task);
            await _unitOfWork.CommitAsync();
            return RedirectToAction(nameof(Index));
        }
    }
}