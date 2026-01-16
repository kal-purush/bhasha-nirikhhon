// <copyright file="WelcomeAdaptiveCard.cs" company="Microsoft">
// Copyright (c) Microsoft. All rights reserved.
// </copyright>

namespace GeneralKnowledgeBot.Helpers.AdaptiveCards
{
    using GeneralKnowledgeBot.Properties;
    using System.Collections.Generic;
    using System.IO;

    public class WelcomeAdaptiveCard
    {
        private static readonly string CardTemplate;

        /// <summary>
        /// Initializes an instance of the <see cref="WelcomeAdaptiveCard"/> class
        /// </summary>
        static WelcomeAdaptiveCard()
        {
            var cardJsonFilePath = Path.Combine(".", "Helpers", "AdaptiveCards", "WelcomeAdaptiveCard.json");
            CardTemplate = File.ReadAllText(cardJsonFilePath);
        }

        public static string GetCard(string botDisplayName)
        {
            var welcomeCardTitleText = Resource.WelcomeCardTitleText;
            var welcomeCardContentPart1 = string.Format(Resource.WelcomeCardContentPart1, botDisplayName);
            var welcomeCardContentPart2 = Resource.WelcomeCardContentPart2;
            var bulletListItem1 = Resource.WelcomeCardBulletListItem1;
            var bulletListItem2 = Resource.WelcomeCardBulletListItem2;
            var bulletListItem3 = Resource.WelcomeCardBulletListItem3;
            var tourIntroText = Resource.TourIntroText;
            var takeATourButtonText = Resource.TakeATourButtonText;

            var variablesToValues = new Dictionary<string, string>()
            {
                { "welcomeCardTitleText", welcomeCardTitleText },
                { "welcomeCardContentPart1", welcomeCardContentPart1 },
                { "welcomeCardContentPart2", welcomeCardContentPart2 },
                { "bulletListItem1", bulletListItem1 },
                { "bulletListItem2", bulletListItem2 },
                { "bulletListItem3", bulletListItem3 },
                { "tourIntroText", tourIntroText },
                { "takeATourButtonText", takeATourButtonText }
            };

            var cardBody = CardTemplate;
            return cardBody;
        }
    }
}