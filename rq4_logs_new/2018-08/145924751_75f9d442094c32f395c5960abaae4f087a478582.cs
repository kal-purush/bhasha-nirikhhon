using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// Corporation class holds all the corporation deatils
/// </summary>
public class Corporation {

    #region Class Attributes

    private int _id;
    private string _name;
    private int _totalStockAvailable;
    private int _stockAvailable;
    private int _tileSize;
    private int _stockValue;
    private bool _isSafe;

    #endregion

    public int Id {
        get { return _id; }
        set { _id = value; }
    }

    public string Name {
        get { return _name; }
        set { _name = value; }
    }

    public int TotalStockAvailable {
        get { return _totalStockAvailable; }
        set { _totalStockAvailable = value; }
    }

    public int StockAvailable {
        get { return _stockAvailable; }
        set {
            if (value < 0)
                _stockAvailable = 0;
            else
                _stockAvailable = value;
        }
    }

    public int TileSize {
        get { return _tileSize; }
        set {
            if (value < 0)
                _tileSize = 0;
            else
                _tileSize = value;
        }
    }

    public int StockValue {
        get { return _stockValue; }
        set {
            if (value < 0)
                _stockValue = 0;
            else
                _stockValue = value;
        }
    }

    public bool IsSafe {
        get { return _isSafe; }
        set { _isSafe = value; }
    }

    public Corporation(int id, string name, int totalStockAvailable = 12, int stockAvailable = 12, int tileSize = 0, int stockValue = 0, bool isSafe = false) {
        _id = id;
        _name = name;
        _totalStockAvailable = totalStockAvailable;
        _stockAvailable = stockAvailable;
        _tileSize = tileSize;
        _stockValue = stockValue;
        _isSafe = isSafe;
    }

    ~Corporation() {
        Debug.Log("Corporation: " + Name + " was removed from game.");
    }
}