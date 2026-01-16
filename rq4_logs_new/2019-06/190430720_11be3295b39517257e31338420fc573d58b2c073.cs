using System.IO;

namespace GeneralKnowledgeBot.Helpers.AdaptiveCards
{
    public class UnrecognizedInputAdaptiveCard
    {
        public static string CardTemplate;

        static UnrecognizedInputAdaptiveCard()
        {
            var cardJsonFilePath = Path.Combine(".", "Helpers", "AdaptiveCards", "UnrecognizedInputAdaptiveCard.json");
            CardTemplate = File.ReadAllText(cardJsonFilePath);
        }

        public static string GetCard()
        {
            var cardBody = CardTemplate;
            return cardBody;
        }
    }
}