namespace Interface
{
    /// <summary>
    /// タイトル画面ViewModelのInterface
    /// </summary>
    public interface ITitleViewModel
    {
        /// <summary>
        /// Playerの名前
        /// </summary>
        string PlayerName { get; }
        
        /// <summary>
        /// Fenyaページに遷移する
        /// </summary>
        void OnClickAttackPageButton();

        /// <summary>
        /// ランキングページに遷移する
        /// </summary>
        void OnClickRankingPageButton();
    }
}