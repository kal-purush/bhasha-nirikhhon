namespace FFXIVVenues.Veni.AI
{
    internal class AIContextBuilder : IAIContextBuilder
    {
        public string GetContext(string id, string chat) 
        {
            string contextPrompt = ContextStrings.Lore;

            contextPrompt += CheckFriendshipStatus(ulong.Parse(id));

            if (chat.Contains("FFXIVenues")) contextPrompt += ContextStrings.MentionsFFXIVVenues;

            contextPrompt += ".Me: " + chat; 

            return "Me: " + contextPrompt + ". You: ";
        }

        private string CheckFriendshipStatus(ulong id)
        {
            string whoIs = "";

            if (id == 236852510688542720)
                whoIs = "Context: My name is Kana, and Im your Mom. ";
            else if (id == 252142384303833088)
                whoIs = "Context: My name is Sumi, and Im your aunt. ";
            else if (id == 880594476295389205)
                whoIs = "Context: My name is Lanna, and Im your friend. ";
            else if (id == 158410288238952449)
                whoIs = "Context: My name is Ali, and Im your friend. ";
            else if (id == 870413151676551178)
                whoIs = "Context: My name is Kaeda, and Im your friend. ";
            else if (id == 894592805689774080)
                whoIs = "Context: My name is Vix, and Im your friend. ";
            else if (id == 265695573527625731)
                whoIs = "Context: My name is Allegro, and Im your friend, and a talking frog";
            else if (id == 786407181804896257)
                whoIs = "Context: My name is Zah, and Im your friend. ";
            else if (id == 99616043571380224)
                whoIs = "Context: My name is Uchu, and Im your friend. ";
            else if (id == 870761195169255525)
                whoIs = "Context: My name is Ada, and Im your friend. ";

            return whoIs;

        }
    }
}