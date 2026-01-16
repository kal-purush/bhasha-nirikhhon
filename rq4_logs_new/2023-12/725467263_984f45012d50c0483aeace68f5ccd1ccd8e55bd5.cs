using Microsoft.AspNetCore.Mvc;
using toiec_web.Services;
using toiec_web.Services.IService;
using toiec_web.ViewModels.Course;
using toiec_web.ViewModels.Question;

namespace toiec_web.Controllers
{
    public class QuestionController : BaseAPIController
    {
        private readonly IQuestionService _questionService;

        public QuestionController(IQuestionService questionService) 
        {
            _questionService = questionService;
        }
        [HttpGet]
        [Route("GetAllQuestions")]
        public async Task<IActionResult> GetAllQuestions()
        {
            var questionList = await _questionService.GetAllQuestions();
            if (questionList == null)
            {
                return StatusCode(StatusCodes.Status404NotFound);
            }
            return Ok(questionList);
        }

        [HttpGet]
        [Route("GetQuestionById/{id:guid}")]
        public async Task<IActionResult> GetQuestionById(Guid id)
        {
            var question = await _questionService.GetQuestionById(id);
            if (question == null)
            {
                return StatusCode(StatusCodes.Status404NotFound);
            }
            return Ok(question);
        }

        [HttpGet]
        [Route("GetAllQuestionByQuiz/{quizId}")]
        public async Task<IActionResult> GetAllQuestionByQuiz(Guid quizId)
        {
            var questionList = await _questionService.GetAllQuestionByQuiz(quizId);
            if (questionList == null)
            {
                return StatusCode(StatusCodes.Status404NotFound);
            }
            return Ok(questionList);
        }

        [HttpGet]
        [Route("GetAllQuestionByUnit/{quizId}")]
        public async Task<IActionResult> GetAllQuestionByUnit(Guid unitId)
        {
            var questionList = await _questionService.GetAllQuestionByUnit(unitId);
            if (questionList == null)
            {
                return StatusCode(StatusCodes.Status404NotFound);
            }
            return Ok(questionList);
        }

        [HttpGet]
        [Route("GetAllQuestionByProfessor/{userId}")]
        public async Task<IActionResult> GetAllQuestionByProfessor(string userId)
        {
            var questionList = await _questionService.GetAllQuestionByProfessor(userId);
            if (questionList == null)
            {
                return StatusCode(StatusCodes.Status404NotFound);
            }
            return Ok(questionList);
        }

        [HttpPost]
        [Route("AddQuestion/{userId}")]
        public async Task<IActionResult> AddQuestion(QuestionAddModel model, string userId)
        {
            var response = await _questionService.AddQuestion(model, userId);
            if (response == true)
            {
                return StatusCode(StatusCodes.Status200OK);
            }
            else
                return StatusCode(StatusCodes.Status500InternalServerError);
        }

        [HttpPut]
        [Route("UpdateCourse/{questionId:guid}&&{userId}")]
        public async Task<IActionResult> UpdateCourse([FromForm] QuestionUpdateModel model, Guid questionId, string userId)
        {
            var response = await _questionService.UpdateQuestion(model, questionId, userId);
            if (response == true)
            {
                return StatusCode(StatusCodes.Status200OK);
            }
            else
                return StatusCode(StatusCodes.Status500InternalServerError);
        }

        [HttpDelete]
        [Route("DeleteCourse/{id:guid}")]
        public async Task<IActionResult> DeleteCourse(Guid id)
        {
            var response = await _questionService.DeleteQuestion(id);
            if (response == true)
            {
                return Ok(response);
            }
            else
                return StatusCode(StatusCodes.Status404NotFound);
        }
    }
}