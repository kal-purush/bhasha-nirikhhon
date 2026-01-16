// <copyright file="ThankYouAdaptiveCard.cs" company="Microsoft">
// Copyright (c) Microsoft. All rights reserved.
// </copyright>

namespace GeneralKnowledgeBot.Helpers.AdaptiveCards
{
    using System.IO;

    /// <summary>
    /// The class responsible for having the Thank You adaptive card generated.
    /// </summary>
    public class ThankYouAdaptiveCard
    {
        private static readonly string CardTemplate;

        /// <summary>
        /// Initializes static members of the <see cref="ThankYouAdaptiveCard"/> class.
        /// </summary>
        static ThankYouAdaptiveCard()
        {
            var cardJsonFilePath = Path.Combine(".", "Helpers", "AdaptiveCards", "ThankYouAdaptiveCard.json");
            CardTemplate = File.ReadAllText(cardJsonFilePath);
        }

        /// <summary>
        /// Method that would return the JSON string of the Thank You adaptive card.
        /// </summary>
        /// <returns>The JSON string for the adaptive card.</returns>
        public static string GetCard()
        {
            var cardBody = CardTemplate;
            return cardBody;
        }
    }
}