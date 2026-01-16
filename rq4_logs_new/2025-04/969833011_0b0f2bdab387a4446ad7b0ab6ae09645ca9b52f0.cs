namespace TextRPG_Team25.Core
{
    public class Player
    {
        public string name = "";
        public string job = "전사";

        public int level = 1;
        public int attack = 10;
        public int defense = 5;
        public int maxHp = 100;
        public int hp = 100; // 현재 체력
        public int gold = 1500;
        // 상태 보기 메서드: 7개 속성만 출력

        internal List<Item> inventory = new List<Item>();

        public void ShowStatus()
        {
            while (true)
            {
                Console.Clear();
                Console.WriteLine("상태 보기\n");

                // 필수 속성 출력
                Console.WriteLine($"Lv.{level:D2}");
                Console.WriteLine($"{name} ( {job} )");
                Console.WriteLine($"공격력 : {attack}");
                Console.WriteLine($"방어력 : {defense}");
                Console.WriteLine($"체력 : {hp} / {maxHp}");
                Console.WriteLine($"Gold : {gold} G\n");

                Console.WriteLine("0. 나가기\n");
                Console.Write(">> ");

                string input = Console.ReadLine();
                if (input == "0")
                    break;

                // 잘못된 입력 처리
                Console.WriteLine("잘못된 입력입니다");
                Console.WriteLine("계속하려면 아무 키나 누르세요...");
                Console.ReadKey();
            }
        }

        public void ShowInventory()  //인벤토리 보기
        {
            Console.Clear();

            // 테스트용 임시 아이템
            AddInventory(0);
            AddInventory(1);

            for (int i = 0; i < inventory.Count; i++)
            {
                inventory[i].ShowItem();
            }
            Console.WriteLine("계속하려면 아무 키나 누르세요...");
            Console.ReadKey();
        }

        public void EquipmentManage()
        {
            Console.WriteLine("장착하거나 해제 하실 장비를 선택하세요");
            string inputNum = Console.ReadLine();
            if (!int.TryParse(inputNum, out int input))
            {
                Console.WriteLine("잘못된 입력입니다.");
                Console.ReadKey();
            }
            if (input == 0)
            {
                Console.WriteLine("메인화면으로 돌아갑니다.");
                Console.ReadKey();
            }
            if (inventory[input - 1].type == ItemType.Potion)
            {
                Console.WriteLine("포션은 장착하거나 해제할 수 없습니다.");
                Console.ReadKey();
            }
            else
            {
                inventory[input - 1].EquipmentItem();
            }
        }

        public void AddInventory(int index) // 아이템 획득 시 실행
        {
            inventory.Add(Item.AddItem(index));
        }
    }
}