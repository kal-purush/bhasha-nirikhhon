using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShopMenu : MonoBehaviour
{
    public static int Money { get; private set; }
    // Start is called before the first frame update
    private Builder builder;

    public TurretPrototype primaryTurret;
    public TurretPrototype longRangeTurret;
    public TurretPrototype basicDefensiveTurret;
    public void BuyPrimaryTurret()
    {
        builder.SetTurretToBuild(primaryTurret);
    }
    public void BuyBasicDefensiveTurret()
    {
        builder.SetTurretToBuild(basicDefensiveTurret);
    }
    public void BuyLongRangeTurretTurret()
    {
        builder.SetTurretToBuild(longRangeTurret);
    }
    void Start()
    {
        Money = 100;
        builder = Builder.Instance;
    }

    // Update is called once per frame
    void Update()
    {

    }
}