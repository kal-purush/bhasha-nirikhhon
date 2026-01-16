using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TextRPG_Team25.UI;
using TextRPG_Team25.Core;

namespace TextRPG_Team25
{
    enum ItemType { portion, defEquip, atKEquip }; // 포션, 방어 장비, 공격 장비
    internal class Item
    {
        public string name;
        public int eff;
        public ItemType type;
        public bool isEquip;
        //public bool hasItem  인벤토리 관련 리스트 작성 시 사용 현재 보류
        Player player;

        public Item(string newName, int newEff, ItemType newType, bool newIsEquip)
        {
            name = newName;
            eff = newEff;
            type = newType;
            isEquip = newIsEquip;
        }

        public static List<Item> items { get; } = new List<Item>
        {
            new Item("무기", 5, ItemType.atKEquip, false),
            new Item("방어구", 5, ItemType.defEquip, false),
            new Item("포션", 5, ItemType.portion, false),
        };

        public void EquipItem()
        {
            switch (type)
            {
                case ItemType.atKEquip:   //플레이어 공격력 증가                  
                    if (!isEquip)
                    {
                        player.attack += eff;
                        isEquip = true;
                    }
                    if (isEquip)
                    {
                        player.attack -= eff;
                        isEquip = false;
                    }
                    break;
                case ItemType.defEquip:  //플레이어 방어력 증가
                    if (!isEquip)
                    {
                        player.defense += eff;
                        isEquip = true;
                    }
                    if (isEquip)
                    {
                        player.defense -= eff;
                        isEquip = false;
                    }
                    break;
                case ItemType.portion:   //플레이어 체력 증가
                    player.hp += eff;
                    break;
            }
        }
    }
}