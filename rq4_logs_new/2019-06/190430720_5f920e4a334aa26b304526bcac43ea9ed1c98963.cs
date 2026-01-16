// <copyright file="IncomingAppFeedbackAdaptiveCard.cs" company="Microsoft">
// Copyright (c) Microsoft. All rights reserved.
// </copyright>

namespace GeneralKnowledgeBot.Helpers.AdaptiveCards
{
    using System.Collections.Generic;
    using System.IO;
    using GeneralKnowledgeBot.Properties;

    /// <summary>
    /// This class will be generating the adaptive card to be sent to the General channel of
    /// the SME team relating to the application feedback.
    /// </summary>
    public class IncomingAppFeedbackAdaptiveCard
    {
        private static readonly string CardTemplate;

        static IncomingAppFeedbackAdaptiveCard()
        {
            var cardJsonFilePath = Path.Combine(".", "Helpers", "AdaptiveCards", "IncomingAppFeedbackAdaptiveCard.json");
            CardTemplate = File.ReadAllText(cardJsonFilePath);
        }

        /// <summary>
        /// Method that returns the JSON string for the adaptive card.
        /// </summary>
        /// <returns>The JSON string.</returns>
        public static string GetCard()
        {
            var cardBody = CardTemplate;
            return cardBody;
        }
    }
}