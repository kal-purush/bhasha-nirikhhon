using UnityEngine;
using UnityEngine.UI;

public class TransactionUI : MonoBehaviour
{
    [SerializeField] private DataSO currencySO;
    [SerializeField] private PriceSO foodPriceSO;
    [SerializeField] private PriceSO waterStonePriceSO;
    [SerializeField] private PriceSO electricStonePriceSO;
    [SerializeField] private PriceSO fireStonePriceSO;

    public void BuyFood ()
    {
        if (currencySO.Value >= foodPriceSO.Value) currencySO.Value -= foodPriceSO.Value;
        // Update inventory
    }

    public void BuyWaterStone ()
    {
        if (currencySO.Value >= waterStonePriceSO.Value) currencySO.Value -= waterStonePriceSO.Value;
        // Update inventory
    }

    public void BuyElectricStone ()
    {
        if (currencySO.Value >= electricStonePriceSO.Value) currencySO.Value -= electricStonePriceSO.Value;
        // Update inventory
    }

    public void BuyFireStone ()
    {
        if (currencySO.Value >= fireStonePriceSO.Value) currencySO.Value -= fireStonePriceSO.Value;
        // Update inventory
    }

}