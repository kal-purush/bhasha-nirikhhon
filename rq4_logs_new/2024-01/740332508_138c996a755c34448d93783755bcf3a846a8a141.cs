using System.Numerics;

namespace spartaDungeon_Help_Update
{
    public class Charater
    {
        public int Lv { get; }
        public string Name { get; } // enum을 이용하면 꼭 string으로 할 필요 없음?
        public string Job { get; }
        public int Atk { get; }
        public int Def { get; }
        public int Health { get; }
        public int Gold { get; }

        public Charater(int lv, string name, string job, int damage, int armor, int health, int gold) // 인스턴스를 생성할 때 기본 세팅
        {
            Lv = lv;
            Name = name;
            Job = job;
            Atk = damage;
            Def = armor;
            Health = health;
            Gold = gold;
        }

    }

    public class Item
    {
        public string itemName { get; }
        public string itemDes { get; }
        public int itemType { get; }
        public int itemAtk { get; }
        public int itemDef { get; }
        public int itemHp { get; }
        public int itemPrice { get; }
        public bool isEquipped { get; set; }
        public bool isPurchased { get; set; }
        // item배열에 아이템 추가를 위한 함수를 만들기 위해서 있는 값. 각각의 아이템에 정의하면 아이템을 세기가 어려워서 클래스에 공유되는 스태틱 변수를 추가하면 아이템이라는 클래스에 귀속이 된다. 학생 수를 구하고 싶으면 학생 하나하나의 특성보다는 전반적인 수를 고려하는 것과 동일. 싱글톤
        public static int ItemCnt = 0;

        public Item(string itemName, string itemDes, int itemType, int itemAtk, int itemDef, int itemHp, int itemPrice, bool isEquipped = false, bool isPurchased = false) // 처음에는 장착x니까 false
        {
            this.itemName = itemName;
            this.itemDes = itemDes;
            this.itemType = itemType;
            this.itemAtk = itemAtk;
            this.itemDef = itemDef;
            this.itemHp = itemHp;
            this.itemPrice = itemPrice;
            this.isEquipped = isEquipped;
            this.isPurchased = isPurchased;
        }

        public void PrintShopItemStatDescription(bool withNumber = false, int idx = 0)
        {
            Console.Write("- ");
            if (withNumber)
            {
                Console.ForegroundColor = ConsoleColor.Magenta;
                Console.Write("{0}. ", idx);
                Console.ResetColor();
            }

            Console.Write(itemName);
            Console.Write(" | ");

            // 3항연산자 참고
            if (itemAtk != 0) Console.Write($"Atk {(itemAtk >= 0 ? "+" : "")}{itemAtk}");
            if (itemDef != 0) Console.Write($"Def {(itemDef >= 0 ? "+" : "")}{itemDef}");
            if (itemHp != 0) Console.Write($"Atk {(itemHp >= 0 ? "+" : "")}{itemHp}");

            Console.Write(" | ");

            Console.Write(itemDes);

            Console.Write(" | ");

            if (isPurchased)
            {
                Console.WriteLine("구매완료");
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Magenta;
                Console.Write(itemPrice);
                Console.ResetColor();
                Console.Write(" G");
                Console.WriteLine();
            }
        }

        public void PrintItemStatDescription(bool withNumber = false, int idx =0)
        {
            Console.Write("- ");
            if (withNumber)
            {
                Console.ForegroundColor = ConsoleColor.Magenta;
                Console.Write("{0}. ", idx);
                Console.ResetColor();
            }

            if (isEquipped)
            {
                Console.Write("[");
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Write("E");
                Console.ResetColor();
                Console.Write("]");
            }
            Console.Write(itemName);
            Console.Write(" | ");

            // 3항연산자 참고
            if (itemAtk != 0) Console.Write($"Atk {(itemAtk >= 0 ? "+" : "")}{itemAtk}");
            if (itemDef != 0) Console.Write($"Def {(itemDef >= 0 ? "+" : "")}{itemDef}");
            if (itemHp != 0) Console.Write($"Atk {(itemHp >= 0 ? "+" : "")}{itemHp}");

            Console.Write(" | ");

            Console.WriteLine(itemDes);
        }
    }

    internal class Program
    {
        static Charater player; // 왜 static 인지? => 정적(static)으로 선언된 변수는 해당 클래스가 처음 사용되는 때에 한 번만 초기화되고 계속 동일한 메모리를 가짐 
        static Item[] playerItems;
        static Item[] items;  // 아이템이 여러개이기 때문에 배열을 사용
        static void Main(string[] args)
        {
            // 구성
            // 0. 데이터 초기화
            // 1. 스타팅로고
            // 2. 선택화면 (상태 인벤 상점)
            // 3. 선택화면 구현(필요 구현 요소 : 캐릭터, 아이템)
            // 4. 인벤화면구현
            // 4-1 장착장비 창 구현
            // 5. 선택화면 저장
            GameDataSetting();
            PrintStartLogo();
            StartMenu();
        }


        static void StartMenu()
        {
            // 구성
            // 0. 화면 정리
            // 1. 선택 멘트를 줌
            // 2. 선택 결과값을 검증함
            // 3. 선택 결과에 따라 메뉴로 보내줌
            Console.Clear();
            Console.WriteLine("스파르타 마을에 오신 여러분 환영합니다\n이곳에서 던전으로 들어가기전 활동을 할 수 있습니다.");
            Console.WriteLine();
            Console.WriteLine("1. 상태 보기\n2. 인벤토리\n3. 상점");
            Console.WriteLine();

            // 1~3의 값을 입력했는지 확인
            // int keyInput = int.Parse(Console.ReadLine()); 이렇게 하면 1~3 이외의 값을 받으면 오류가 생긴다.
            switch (CheckValidInput(1, 3))
            {
                case 1:
                    StatusMenu();
                    break;
                case 2:
                    InventoryMenu();
                    break;
                case 3:
                    ShopMenu();
                    break;
            }
        }

        private static void StatusMenu()
        {
            Console.Clear();

            ShowHighlightedText("상태 보기");
            Console.WriteLine("캐릭터의 정보가 표시됩니다.");
            Console.WriteLine() ;
            PrintTextWithHighlight("Lv. ", player.Lv.ToString("00")); // 숫자 2자리까지 표시
            Console.WriteLine("{0} ( {1} )", player.Name, player.Job);

            int bonusAtk = getSumBonusAtk();
            PrintTextWithHighlight("공격력 : ", (player.Atk + bonusAtk).ToString(), bonusAtk > 0 ? string.Format(" (+{0})", bonusAtk) : "");
            int bonusDef = getSumBonusDef();
            PrintTextWithHighlight("방어력 : ", (player.Def + bonusDef).ToString(), bonusDef > 0 ? string.Format(" (+{0})", bonusDef) : "");
            int bonusHp = getSumBonusHp();
            PrintTextWithHighlight("체력 : ", (player.Health + bonusHp).ToString(), bonusHp > 0 ? string.Format(" (+{0})", bonusHp) : "");
            PrintTextWithHighlight("골드 : ", player.Gold.ToString());
            Console.WriteLine();
            Console.WriteLine("0. 뒤로가기");
            Console.WriteLine();
            switch (CheckValidInput(0, 0))
            {
                case 0:
                    StartMenu();
                    break;
            }
        }

        private static int getSumBonusAtk()
        {
            int sum = 0;
            for (int i = 0; i < Item.ItemCnt; i++)
            {
                if (items[i].isEquipped) sum += items[i].itemAtk;
            }
            return sum;
        }

        private static int getSumBonusDef()
        {
            int sum = 0;
            for (int i = 0; i < Item.ItemCnt; i++)
            {
                if (items[i].isEquipped) sum += items[i].itemDef;
            }
            return sum;
        }

        private static int getSumBonusHp()
        {
            int sum = 0;
            for (int i = 0; i < Item.ItemCnt; i++)
            {
                if (items[i].isEquipped) sum += items[i].itemHp;
            }
            return sum;
        }



        private static void InventoryMenu()
        {
            Console.Clear();

            ShowHighlightedText("인벤토리");
            Console.WriteLine("보유 중인 아이템을 관리할 수 있습니다.");
            Console.WriteLine();
            Console.WriteLine("[아이템 목록]");

            for (int i = 0; i < Item.ItemCnt; i++)
            {
                items[i].PrintItemStatDescription();
            }
            Console.WriteLine();
            Console.WriteLine("1. 장착관리");
            Console.WriteLine("0. 나가기");
            Console.WriteLine();
            switch (CheckValidInput(0, 1))
            {
                case 0:
                    StartMenu();
                    break;
                case 1:
                    EquipMenu();
                    break;
            }
        }

        private static void EquipMenu()
        {
            Console.Clear();

            ShowHighlightedText("인벤토리 - 장착 관리");
            Console.WriteLine("보유중인 아이템을 관리할 수 있습니다.");
            Console.WriteLine();
            Console.WriteLine("[아이템 목록]");
            for (int i = 0; i < Item.ItemCnt; i++)
            {
                items[i].PrintItemStatDescription(true, i + 1); // 인덱스에는 0,1,2로 들어가지만 출력은 1,2,3
            }
            Console.WriteLine();
            Console.WriteLine("0. 나가기");

            int keyInput = CheckValidInput(0, Item.ItemCnt);

            switch (keyInput)
            {
                case 0:
                    InventoryMenu();
                    break;
                default:
                    ToggleEquipStatus(keyInput - 1); //유저가 입력하는 건 1,2,3 실제 배열은 0,1,2
                    EquipMenu();
                    break;
            }

        }

        private static void ToggleEquipStatus(int idx)
        {
            items[idx].isEquipped = !items[idx].isEquipped;
        }

        private static void CheckIsPurchased(int idx) 
        {
            items[idx].isPurchased = !items[idx].isPurchased;
        }

        private static void ShopMenu()
        {
            Console.Clear();

            ShowHighlightedText("상점");
            Console.WriteLine("필요한 아이템을 얻을 수 있는 상점입니다.");
            Console.WriteLine();
            Console.WriteLine("[보유 골드]");
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.Write(player.Gold);
            Console.ResetColor();
            Console.Write(" G");
            Console.WriteLine();

            Console.WriteLine("[아이템 목록]");
            Console.WriteLine();
            for (int i = 0; i < Item.ItemCnt; i++)
            {
                items[i].PrintShopItemStatDescription();
            }
            Console.WriteLine();
            Console.WriteLine("1. 아이템 구매");
            Console.WriteLine("0. 나가기");
            Console.WriteLine();
            switch (CheckValidInput(0, 1))
            {
                case 0:
                    StartMenu();
                    break;
                case 1:
                    ShopTradeMenu();
                    break;
            }


        }

        private static void ShopTradeMenu()
        {
            Console.Clear();

            ShowHighlightedText("상점 - 아이템 구매");
            Console.WriteLine("필요한 아이템을 얻을 수 있는 상점입니다.");
            Console.WriteLine();
            Console.WriteLine("[보유 골드]");
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.Write(player.Gold);
            Console.ResetColor();
            Console.Write(" G");
            Console.WriteLine();
            Console.WriteLine("[아이템 목록]");
            for (int i = 0; i < Item.ItemCnt; i++)
            {
                items[i].PrintShopItemStatDescription(true, i + 1); // 인덱스에는 0,1,2로 들어가지만 출력은 1,2,3
            }
            Console.WriteLine();
            Console.WriteLine("0. 나가기");

            int keyInput = CheckValidInput(0, Item.ItemCnt);

            switch (keyInput)
            {
                case 0:
                    InventoryMenu();
                    break;
                default:
                    ToggleEquipStatus(keyInput - 1); //유저가 입력하는 건 1,2,3 실제 배열은 0,1,2
                    EquipMenu();
                    break;
            }
        }

        private static void ShowHighlightedText(string text)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(text);
            Console.ResetColor();
        }

        // 중간 컬러 변, 뒤에 빈칸이 와도 상관없게
        private static void PrintTextWithHighlight(string s1, string s2, string s3 = "")
        {
            Console.Write(s1);
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.Write(s2);
            Console.ResetColor();
            Console.WriteLine(s3);
        }

        private static int CheckValidInput(int min, int max)
        {
            // 설명
            // 아래 두 가지 상황은 비정상 -> 재입력 수행
            // 1. 숫자가 아닌 값 입력
            // 2. 숫자가 1~3 범위를 벗어남
            int keyInput; // 미리 정의하는 이유는 Try parse에 필요하기 때문
            bool result; // while문에 필요하기 때문
            while (true)
            {
                Console.WriteLine("원하시는 행동을 입력해주세요");
                result = int.TryParse(Console.ReadLine(), out keyInput); // tryparse : 될수도 안될수도 있다고 생각
                if (result == false || CheckIfValid(keyInput, min, max) == false) // 틀리면 계속 반복, 키인풋이 숫자인지 체크,1~3인지 체크
                {
                    Console.ForegroundColor= ConsoleColor.Red;
                    Console.WriteLine("잘못된 입력입니다.");
                    Console.ResetColor();
                }
                else break;

            } 
            //제대로 키 입력을 받았으면
            return keyInput;
        }

        private static bool CheckIfValid(int keyInput, int min, int max)
        {
            if (min <= keyInput && keyInput <= max) return true;
            return false;
        }

        private static void PrintStartLogo() // 
        {
            /*
            Console.WriteLine("░█▀▀░█▀█░█▀█░█▀▄░▀█▀░█▀█");  // ??? 로 나와서 주석처리함 encoding문제?
            Console.WriteLine("░▀▀█░█▀▀░█▀█░█▀▄░░█░░█▀█");
            Console.WriteLine("░▀▀▀░▀░░░▀░▀░▀░▀░░▀░░▀░▀");
            Console.WriteLine("░█▀▄░█░█░█▀█░█▀▀░█▀▀░█▀█░█▀█");
            Console.WriteLine("░█░█░█░█░█░█░█░█░█▀▀░█░█░█░█");
            Console.WriteLine("░▀▀░░▀▀▀░▀░▀░▀▀▀░▀▀▀░▀▀▀░▀░▀");
            */
            Console.WriteLine("==============================");
            Console.WriteLine("     PRESS ANYKEY TO START    ");
            Console.WriteLine("==============================");
            Console.ReadKey();
        }



        private static void GameDataSetting()
        {
            player = new Charater(1, "rtan", "전사", 10, 5, 100, 1500);
            playerItems = new Item[100];
            items = new Item[100];
            AddItem(new Item("수련자 갑옷", "수련에 도움을 주는 갑옷입니다.", 0, 0, 5, 0, 1000));
            AddItem(new Item("무쇠갑옷", "무쇠로 만들어져 튼튼한 갑옷입니다.", 0, 0, 9, 0, 2400));
            AddItem(new Item("스파르타의 갑옷", "스파르타의 전사들이 사용했다는 전설의 갑옷입니다.", 0, 0, 15, 0, 3500));
            AddItem(new Item("낡은 검", "쉽게 볼 수 있는 낡은 검 입니다.", 1, 2, 0, 0, 600));
            AddItem(new Item("청동 도끼", "어디선가 사용됐던거 같은 도끼입니다.", 1, 5, 0, 0, 1500));
            AddItem(new Item("스파르타의 창", "스파르타의 전사들이 사용했다는 전설의 창입니다.", 1, 7, 0, 0, 2100));

        }

        static void AddItem(Item item)
        {
            if (Item.ItemCnt == 100) return; // 아이템이 꽉찬 경우 아무일도 일어나지 않음.
            items[Item.ItemCnt] = item; // 아이템 0개인 경우 -> 0번인덱스  1개인 경우 -> 1번인덱스
            Item.ItemCnt++;
        }
    }
}