using UnityEngine;
using System;

public class CargoCeramic : ICargo {
    #region ICargo implementation
    public UnityEngine.Sprite cargoImage {
        get {
            return GameObject.Find("Entities").GetComponent<Entities>().cargoCeramicSprite;
        }
    }
    public string cargoTitle {
        get {
            return "Ceramics";
        }
    }
    public string cargoBody {
        get {
            return String.Format("Fragile. Sell for {0}, but breaks on damage.", sellprice);
        }
    }
    public global::CargoType cargoType {
        get {
            return CargoType.Ceramic;
        }
    }
    public int price {
        get {
            return Entities.Price(CargoType.Ceramic);
        }
    }
    public int sellprice {
        get {
            return Entities.SellPrice(CargoType.Ceramic);
        }
    }
    public UnityEngine.GameObject cargoFireObject {
        get {
            return GameObject.Find("Entities").GetComponent<Entities>().emptyObject;
        }
    }
    public float cargoFireTimer {
        get {
            return 9999;
        }
    }
    #endregion
}