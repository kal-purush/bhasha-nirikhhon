using Interface;
using ValueObject;

namespace ViewModel
{
    /// <summary>
    /// ランキングカード表示のためのViewModel
    /// </summary>
    public class RankingCardViewModel : IRankingCardViewModel
    {
        public int Rank { get; private set; }
        public string Name { get; private set; }
        public int TotalDamage { get; private set; }
        public bool IsSelf { get; private set; }

        public RankingCardViewModel(int rank, PlayerVO playerVo, DamageVO damageVo)
        {
            Rank = rank;
            Name = playerVo.Name;
            TotalDamage = damageVo.Amount;
            IsSelf = playerVo.IsSelf;
        }
    }
}