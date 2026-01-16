namespace GeneralKnowledgeBot.Helpers.AdaptiveCards
{
    using GeneralKnowledgeBot.Models;
    using GeneralKnowledgeBot.Properties;
    using System.Collections.Generic;
    using System.IO;

    public class ResponseAdaptiveCard
    {
        public static string CardTemplate;

        public ResponseAdaptiveCard()
        {
            var cardJsonFilePath = Path.Combine(".", "Helpers", "AdaptiveCards", "ResponseAdaptiveCard.json");
            CardTemplate = File.ReadAllText(cardJsonFilePath);
        }

        public static string GetCard(string question, Response model)
        {
            var questionLineText = string.Format(Resource.QuestionLineText, question);
            var responseCardTitleText = Resource.ResponseCardTitleText;
            var answerLineText = string.Format(Resource.AnswerLineText, model.answers[0].answer);

            var variablesToValues = new Dictionary<string, string>()
            {
                { "responseCardTitleText", responseCardTitleText },
                { "questionLineText", questionLineText },
                { "answerLineText", answerLineText }
            };

            var cardBody = CardTemplate;
            foreach (var kvp in variablesToValues)
            {
                cardBody = cardBody.Replace($"%{kvp.Key}%", kvp.Value);
            }

            return cardBody;
        }
    }
}