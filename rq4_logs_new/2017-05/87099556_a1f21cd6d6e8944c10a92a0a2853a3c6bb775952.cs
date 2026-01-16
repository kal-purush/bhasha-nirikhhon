using System.Numerics;

namespace BlizzardAPI.SC2.DataResources
{
    public class Animation
    {
        public string Title { get; set; }
        public string Command { get; set; }
        public BigInteger Id { get; set; }
        public Icon Icon { get; set; }
        public BigInteger AchievementId { get; set; }
    }
}