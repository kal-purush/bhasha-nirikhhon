using Manager;
using PageSettings;
using ViewModel;

namespace Model
{
    public class Ranking
    {
        private readonly RankingViewModel viewModel;

        public Ranking()
        {
            viewModel = new RankingViewModel(this);
        }

        /// <summary>
        /// 画面を表示する
        /// </summary>
        public void Show()
        {
            var uiManager = UIManager.Instance;
            var pageSetting = uiManager.GetPageSetting<RankingSetting>();
            pageSetting.Bind(viewModel);
            uiManager.ReplaceCurrentPage<RankingSetting>();
        }
    }
}