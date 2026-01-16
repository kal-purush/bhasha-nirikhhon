using TextRPG_Team25.Core;
using TextRPG_Team25.UI;

namespace TextRPG_Team25
{

    public class Shop()
    {
        private List<Item> shopItems = Item.items.Where(i => !i.isPurchase).ToList();
        public Utils utils = new Utils();

        public void ShowShop(Player buyer)
        {
            Console.Clear();
            utils.PrintItems(Item.items, true, false, true, true);
            Console.WriteLine();
            Utils.MenuOption("1", "아이템 구매하기");
            Utils.MenuOption("2", "아이템 판매하기");
            Console.WriteLine();
            Utils.MenuOption("0", "상점 나가기");
            Console.Write("\n>> ");

            string input = Console.ReadLine();

            switch (input)
            {
                case "1":
                    BuyItem(buyer);
                    break;
                case "2":
                    Console.WriteLine("판매 기능은 준비 중입니다.");
                    Console.ReadKey();
                    break;
                case "0":
                    Console.WriteLine("상점을 떠납니다.");
                    Console.ReadKey();
                    return;
                default:
                    Console.WriteLine("잘못된 입력입니다.");
                    Console.ReadKey();
                    break;
            }

            Console.ReadKey();
        }


        public void BuyItem(Player buyer)
        {
            Console.Clear();
            Console.WriteLine("[ 아이템 구매 ]\n");

            utils.PrintItems(Item.items, true, false, true, true);

            Console.WriteLine();
            Console.Write("구매할 아이템 번호를 입력하세요.\n>> ");
            string rawInput = Console.ReadLine();

            if (int.TryParse(rawInput, out int input))
            {
                int index = input - 1;

                if (index < 0 || index >= shopItems.Count)
                {
                    Console.WriteLine("상점에 없는 물품입니다.");
                    Console.ReadKey();
                    return;
                }
                
                Item itemToBuy = shopItems[index];
               
                if (buyer.gold < itemToBuy.price)
                {
                    Console.WriteLine("골드가 부족합니다.");
                    Console.ReadKey();
                    return;
                }

                buyer.gold -= itemToBuy.price;
                buyer.AddInventory(itemToBuy.id);
                Console.WriteLine($"{itemToBuy.name}을(를) 구매했습니다!");
                Console.ReadKey();
            }
            else
            {
                Console.WriteLine("잘못된 입력입니다.");
                Console.ReadKey();
            }
        }
    }
}