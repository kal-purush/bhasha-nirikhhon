using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// Wallet class holds the player id/Player and the amount of money
/// </summary>
public class Wallet {
    // Set class attributes
    #region Class Attributes
    
    private int _player;
    private int _amount;

    public int Player {
        get { return _player; }
        set { _player = value; }
    }

    public int Amount {
        get { return _amount; }
        set {
            if (value < 0)
                _amount = 0;
            else
                _amount = value;
        }
    }

    public Wallet(int player, int amount) {
        _player = player;
        _amount = amount;
    }

    #endregion
}

/// <summary>
/// The MoneyController is what controls who has what, it's a single point of truth versus on each player
/// </summary>
public class MoneyController : MonoBehaviour {

    #region Class Imports

    //public PlayerController players;

    #endregion

    // Set class attributes
    #region Class Attributes

    private List<Wallet> wallets;

    #endregion

    #region Unity Specific

    // Use this for initialization
    void Start() {
        // TODO: Pass players or player id
        CreateWallets(3);
    }

    // Update is called once per frame
    void Update() {

    }

    #endregion

    #region Class Functions

    /// <summary>
    /// Create wallets for each player
    /// </summary>
    /// <param name="players">Players or int</param>
    public void CreateWallets(int players) {
        for (int i = 0; i < players; ++i)
            wallets.Add(new Wallet(i, 6000));
    }

    /// <summary>
    /// Get the player's amount of money in their wallet
    /// </summary>
    /// <param name="player">Id of player or Player</param>
    /// <returns>int amount</returns>
    public int PlayerAmount(int player) {
        Wallet playerWallet = wallets.Find(wallet => wallet.Player == player);
        return playerWallet.Amount;
    }

    /// <summary>
    /// Increase the amount of money in player's wallet
    /// </summary>
    /// <param name="player">Id of player or Player</param>
    /// <returns>int amount</returns>
    public int GainMoney(int player, int earned) {
        Wallet playerWallet = wallets.Find(wallet => wallet.Player == player);
        playerWallet.Amount += earned;
        return playerWallet.Amount;
    }

    /// <summary>
    /// Reduce the amount of money in player's wallet
    /// </summary>
    /// <param name="player">Id of player or Player</param>
    /// <returns>int amount</returns>
    public int SpendMoney(int player, int spent) {
        Wallet playerWallet = wallets.Find(wallet => wallet.Player == player);
        playerWallet.Amount -= spent;
        return playerWallet.Amount;
    }

    #endregion
}