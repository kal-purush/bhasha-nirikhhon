using System;

namespace SpaceGame
{
    class TimeEvent : RandomEvent
    {
        // Property getters scale amount of time lost/gained with difficulty
        int timeLost => new Random().Next(difficulty / 3, difficulty * 2);
        int timeGained => new Random().Next(difficulty / 2, difficulty * 2);

        // Use the base class constructor
        public TimeEvent(Difficulty difficulty) : base(difficulty) { }

        protected override string NegativeEvent(Ship ship)
        {
            ship.Time -= timeLost;
            return GetNegativeEventMessage();
        }

        protected override string PositiveEvent(Ship ship)
        {
            ship.Time += timeGained;
            return GetPositiveEventMessage();
        }

        protected override string GetNegativeEventMessage()
        {
            string[] timeMessages = new string[]
            {
                $"While watching the news, you see that the nations of earth are at war! The destruction of Earth is now {timeLost} days faster!",
                $"Oh no! The aliens invading earth have destroyed the moon! -{timeLost} days to Earth's destruction.",
                $"You realize that this whole time you have been looking at the wrong calender, and that Earth's destruction is actually {timeLost} days closer.",
                $"While watching the news, you see that the alien force invading Earth has destroyed her orbital defense sytem! -{timeLost} days until Earth's destruction."
            };

            return timeMessages[new Random().Next(0, timeMessages.Length)];
        }

        protected override string GetPositiveEventMessage()
        {
            string[] timeMessages = new string[]
            {
                $"After some contemplation, you realize that Earth's destruction is no big deal, and allow yourself an extra {timeGained} days.",
                $"While watching the news, you see that the lunar defense corps has re-taken the moon! (or whats left of it anyway). Looks like they bought you {timeGained} days.",
                $"In the last issue of Galactic Times you read that the commander of the alien fleet has been assassinated, buying Earth {timeGained} more days!",
                $"The Space Force has successfully retaken Mars, a key defense point in Earths solar system. Earth's destruction has been delayed by {timeGained} days."
            };

            return timeMessages[new Random().Next(0, timeMessages.Length)];
        }
    }
}