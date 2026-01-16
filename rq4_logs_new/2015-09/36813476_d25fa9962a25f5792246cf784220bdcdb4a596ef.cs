namespace Twitch_Exploit
{
    class Program
    {
        private static readonly System.Media.SoundPlayer Exploit = new System.Media.SoundPlayer(Properties.Resources.exploit);
        private static void Main()
        {
            LeagueSharp.Common.CustomEvents.Game.OnGameLoad += args => Exploit.PlayLooping();
        }
    }
}