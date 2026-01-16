using Microsoft.AspNetCore.Mvc;
using DevQuestion.Contracts;
namespace DevQuestions.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public partial class QuestionsController : ControllerBase
    {

        [HttpPost]
        public async Task<IActionResult> Create([FromBody] CreateQuestionDto request, CancellationToken cancellationToken)
        {
            return Ok("Question created");
        }
        [HttpGet]
        public async Task<IActionResult> Get([FromQuery] GetQuestionsDto request, CancellationToken cancellationToken)
        {
            return Ok("Questions Get");
        }
        [HttpGet("{questionId:guid}")]
        public async Task<IActionResult> GetById([FromRoute]Guid questionId, CancellationToken cancellationToken)
        {
            return Ok("Questions Get");
        }
        [HttpPut("{questionId:guid}")]
        public async Task<IActionResult> Update([FromRoute] Guid questionId,[FromBody] UpdateQuestionDto request,  CancellationToken cancellationToken)
        { 
            return Ok("Question updated");
        }
        [HttpDelete("{questionId:guid}")]
        public async Task<IActionResult> Delete([FromRoute] Guid questionId, CancellationToken cancellationToken)
        {
            return Ok("Question deleted");  
        }
        [HttpPut("{questionId:guid}/solution")]
        public async Task<IActionResult> SelectSolution([FromRoute] Guid questionId, [FromQuery] Guid answerIdm, CancellationToken cancellationToken)
        {
            return Ok("Solution Selected");
        }
        [HttpPost("{questionId:guid}/answers")]
        public async Task<IActionResult> AddAnswer([FromRoute] Guid questionId, [FromBody] AddAnswerDto request , CancellationToken cancellationToken)
        {
            return Ok("Answer added");
        }
    }
  }