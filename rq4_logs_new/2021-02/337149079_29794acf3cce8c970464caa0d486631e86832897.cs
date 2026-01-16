using System;
using System.Collections.Generic;
using System.Text;

namespace SpaceGame
{
    // Doesn't actually do anything to the ship, just returns a funny message. However, these are more rare and are independant
    // of difficulty level

    class EasterEggEvent : RandomEvent
    {
        protected override bool shouldTrigger => 5 > new Random().Next(0, 100);
        protected override bool shouldTriggerPositive => 5 > new Random().Next(0, 100);

        public EasterEggEvent(Difficulty difficulty) : base(difficulty) { }

        protected override string NegativeEvent(Ship ship) => GetNegativeEventMessage();

        protected override string PositiveEvent(Ship ship) => GetPositiveEventMessage();

        protected override string GetNegativeEventMessage()
        {
            string[] easterEggMessages = new string[]
            {
                "Why did the cow go in the spaceship? It wanted to see the mooooooooon."
            };

            return easterEggMessages[new Random().Next(0, easterEggMessages.Length)];
        }

        protected override string GetPositiveEventMessage()
        {
            string[] easterEggMessages = new string[]
            {
                "Why did people not like the restaurant on the moon? Because there was no atmosphere."
            };

            return easterEggMessages[new Random().Next(0, easterEggMessages.Length)];
        }
    }
}