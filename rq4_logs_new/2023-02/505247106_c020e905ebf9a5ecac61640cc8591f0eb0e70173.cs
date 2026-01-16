using FFXIVVenues.Veni.Context;
using FFXIVVenues.Veni.Utils;

namespace FFXIVVenues.Veni.AI
{
    internal class AIHandler
    {
        public string responseHandler(InteractionContext context)
        {
            var messageContent = context.Interaction.Content.Replace($"<@994410006638239795> ", "").Trim();
            var id = context.Interaction.User.Id.ToString();

            return new DavinciProxy().askTheAI("Me: " + new AIRepository().getMyLore(id, messageContent) + ". You: ");
        } 
        
    }
}