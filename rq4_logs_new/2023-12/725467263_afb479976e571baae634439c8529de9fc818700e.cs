using Microsoft.AspNetCore.Mvc;
using OpenAI_API.Completions;
using OpenAI_API;

namespace toiec_web.Controllers
{
    public class ChatGPTMessageController : BaseAPIController
    {
        public ChatGPTMessageController() 
        {
            
        }

        [HttpPost]
        public async Task<IActionResult> GetChatGPTMessage(string searchText)
        {
            //string APIKey = "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";
            //string APIKey = "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";
            string APIKey = "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";
            string answer = string.Empty;

            var openai = new OpenAIAPI(APIKey);
            CompletionRequest completion = new CompletionRequest();
            completion.Prompt = searchText;
            completion.Model = OpenAI_API.Models.Model.DavinciText;
            completion.MaxTokens = 4000;

            var result = openai.Completions.CreateCompletionAsync(completion);
            foreach(var item in result.Result.Completions)
            {
                answer = item.Text;
            }
            return Ok(answer);
        }
    }
}