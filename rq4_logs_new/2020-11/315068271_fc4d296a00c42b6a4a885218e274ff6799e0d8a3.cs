using Fluxor;

namespace DeusVultClicker.Client.Upgrade.Store.Selector
{
    public class UpgradeEffectsSelector
    {
        private readonly IState<UpgradeState> upgradeState;

        public UpgradeEffectsSelector(IState<UpgradeState> upgradeState)
        {
            this.upgradeState = upgradeState;
        }
        public double SelectMoneyPerFollowerIncrease()
        {
            return UpgradeEffectsSelectorHelper.SelectMoneyPerFollowerIncrease(upgradeState.Value.PurchasedUpgradeIds);
        }
        public double SelectFaithPerFollowerIncrease()
        {
            return UpgradeEffectsSelectorHelper.SelectFaithPerFollowerIncrease(upgradeState.Value.PurchasedUpgradeIds);

        }
        public double SelectFaithPerClickIncrease()
        {
            return UpgradeEffectsSelectorHelper.SelectFaithPerClickIncrease(upgradeState.Value.PurchasedUpgradeIds);
        }
       
    }
}