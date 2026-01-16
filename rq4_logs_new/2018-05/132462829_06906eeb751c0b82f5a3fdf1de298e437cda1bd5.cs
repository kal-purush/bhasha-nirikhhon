using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Card {
    Player theplayer;
    ModelContainer container;
    public GameObject CardModel;

    /// <summary>
    /// 卡牌名称
    /// </summary>
    private string Name;

    /// <summary>
    /// 卡牌类型：英雄牌/效果牌
    /// </summary>
    private int CardType;

    /// <summary>
    /// 卡牌耗费
    /// </summary>
    private int Cost;

    /// <summary>
    /// 效果牌专有：卡牌效果
    /// </summary>
    private Effect effect;

    /// <summary>
    /// 英雄牌专有：英雄血量
    /// </summary>
    private int HeroHP;

    /// <summary>
    /// 英雄牌专有：英雄伤害
    /// </summary>
    private int HeroDamage;

    /// <summary>
    /// 英雄牌专有：英雄效果
    /// </summary>
    private Effect HeroEffect;

    public Card(string name, int cost, int herohp, int herodamage)
    {
        Name = name;
        Cost = cost;
        HeroHP = herohp;
        HeroDamage = herodamage;
        theplayer = GameObject.Find("Main Camera").GetComponent<Player>();
        container = GameObject.Find("Main Camera").GetComponent<ModelContainer>();
        CardModel = container.CardModel;
        // 临时属性
        CardType = 0;
    }

    private void OnMouseDown()
    {
    }

    /// <summary>
    /// 施展效果，通过effect.CastEffect()实现
    /// </summary>
    public void CastEffect()
    {

    }

    /// <summary>
    /// 根据卡牌属性生成一个英雄实例并返回
    /// </summary>
    /// <returns>英雄实例，用于被玩家召唤</returns>
    public Hero InitHero()
    {
        Hero hero = new Hero(Name, HeroHP, HeroDamage);
        return hero;
    }

    public string GetName()
    {
        return Name;
    }

    public int GetCost()
    {
        return Cost;
    }

    // 临时方法
    public int getCardType()
    {
        return CardType;
    }
}