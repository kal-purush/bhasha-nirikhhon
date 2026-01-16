using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class HudController : MonoBehaviour {

    private Text _playerNameText = GameObject.Find("PlayerName").GetComponent<Text>();
    private Text _walletAmountText = GameObject.Find("PlayerWalletAmount").GetComponent<Text>();
    private Text _nestorStockText = GameObject.Find("PlayerStockNestor").GetComponent<Text>();
    private Text _sparkStockText = GameObject.Find("PlayerStockSpark").GetComponent<Text>();
    private Text _etchStockText = GameObject.Find("PlayerStockEtch").GetComponent<Text>();
    private Text _roveStockText = GameObject.Find("PlayerStockRove").GetComponent<Text>();
    private Text _fleetStockText = GameObject.Find("PlayerStockFleet").GetComponent<Text>();
    private Text _echoStockText = GameObject.Find("PlayerStockEcho").GetComponent<Text>();
    private Text _boltStockText = GameObject.Find("PlayerStockBolt").GetComponent<Text>();

    public void SetPlayerName (string newName) {
        _playerNameText.text = newName;
    }

    public void SetWalletAmount(int newAmount) {
        _walletAmountText.text = newAmount.ToString();
    }

    public void UpdatePlayerStock(List<Stock> stocks) {
        _nestorStockText.text = stocks.FindAll(stock => stock.CorporationId.Equals(0)).Count.ToString();
        _sparkStockText.text = stocks.FindAll(stock => stock.CorporationId.Equals(1)).Count.ToString();
        _etchStockText.text = stocks.FindAll(stock => stock.CorporationId.Equals(2)).Count.ToString();
        _roveStockText.text = stocks.FindAll(stock => stock.CorporationId.Equals(3)).Count.ToString();
        _fleetStockText.text = stocks.FindAll(stock => stock.CorporationId.Equals(4)).Count.ToString();
        _echoStockText.text = stocks.FindAll(stock => stock.CorporationId.Equals(5)).Count.ToString();
        _boltStockText.text = stocks.FindAll(stock => stock.CorporationId.Equals(6)).Count.ToString();
    }

}