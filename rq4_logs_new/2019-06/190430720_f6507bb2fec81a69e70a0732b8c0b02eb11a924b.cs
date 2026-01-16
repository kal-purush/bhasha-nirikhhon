// <copyright file="AskAnExpertAdaptiveCard.cs" company="Microsoft">
// Copyright (c) Microsoft. All rights reserved.
// </copyright>

namespace GeneralKnowledgeBot.Helpers.AdaptiveCards
{
    using System.IO;

    /// <summary>
    /// The class responsible for the generation of the ask an expert adaptive card.
    /// </summary>
    public class AskAnExpertAdaptiveCard
    {
        private static readonly string CardTemplate;

        /// <summary>
        /// Initializes static members of the <see cref="AskAnExpertAdaptiveCard"/> class.
        /// </summary>
        static AskAnExpertAdaptiveCard()
        {
            var cardJsonFilePath = Path.Combine(".", "Helpers", "AdaptiveCards", "AskAnExpertAdaptiveCard.json");
            CardTemplate = File.ReadAllText(cardJsonFilePath);
        }

        /// <summary>
        /// Method that will return the JSON string of the adaptive card.
        /// </summary>
        /// <returns>JSON string.</returns>
        public static string GetCard()
        {
            var cardBody = CardTemplate;
            return cardBody;
        }
    }
}