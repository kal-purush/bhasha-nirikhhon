// <copyright file="WelcomeTeamAdaptiveCard.cs" company="Microsoft">
// Copyright (c) Microsoft. All rights reserved.
// </copyright>

namespace GeneralKnowledgeBot.Helpers.AdaptiveCards
{
    using System;
    using System.IO;

    /// <summary>
    /// The class for the adaptive card to be shown in a team.
    /// </summary>
    public class WelcomeTeamAdaptiveCard
    {
        private static readonly string CardTemplate;

        /// <summary>
        /// Initializes static members of the <see cref="WelcomeTeamAdaptiveCard"/> class.
        /// </summary>
        static WelcomeTeamAdaptiveCard()
        {
            var cardJsonFilePath = Path.Combine(".", "Helpers", "AdaptiveCards", "WelcomeTeamAdaptiveCard.json");
            CardTemplate = File.ReadAllText(cardJsonFilePath);
        }

        /// <summary>
        /// Method that returns the welcome team adaptive card.
        /// </summary>
        /// <param name="botDisplayName">The bot display name.</param>
        /// <returns>The JSON string for the adaptive card.</returns>
        public static string GetCard(string botDisplayName)
        {
            throw new NotImplementedException();
        }
    }
}