using UnityEngine;
using System.Collections;

public class AnimalStatusManager : MonoBehaviour
{
    public enum Sexuality
    {
        MALE,
        FEMALE,
    }

    public struct AnimalStatus
    {
        // ID
        public int ID { get; set; }
        // 名前
        public string Name { get; set; }
        // 購入金額
        public int PurchasePrice { get; set; }
        // 一日にかかる食費
        public int FoodCost { get; set; }
        // 満腹度
        public int SatietyLevel { get; set; }
        // 愛情度
        public int LoveDegree { get; set; }
        // 次のレベルまでの値
        public int NextLevel { get; set; }
        // 呼び込める客の数
        public int AttractVisitors { get; set; }
        // 倍率
        public float Ratio { get; set; }
        // 性別
        public Sexuality Sexuality { get; set; }
        // レア度
        public int Rarity { get; set; }
        // 檻の番号
        // 控えは99
        public int CageID { get; set; }
    }
    public AnimalStatus status;

    // 購入してあるか
    public bool IsPurchase { get; set; }
    // 檻に入っているかどうか
    public bool IsActive { get; set; }


    void Start()
    {
        InitStatus();
    }

    void InitStatus()
    {
        status.ID = 99;
        status.Name = "***";
        status.PurchasePrice = 0;
        status.FoodCost = 0;
        status.SatietyLevel = 100;
        status.LoveDegree = 1;
        status.NextLevel = 0;
        status.AttractVisitors = 0;
        status.Ratio = 1.0f;
        status.Sexuality = Sexuality.MALE;
        status.Rarity = 1;
        IsPurchase = false;
        IsActive = false;
    }


    // 経過した時間分の来場客数の計算
    //＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊
    // プランナーに用相談
    //＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊
    public int GetVisitors(float elapsedTime)
    {
        float visitors = (float)status.AttractVisitors * status.Ratio* elapsedTime;

        return (int)visitors;
    }
}