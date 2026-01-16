using System;
namespace team4_assignment
{
	public class Character
	{
		
        Entrance entrance = new Entrance();
        public string UserName { get; private set; }
        

        public Character()
		{

        }

		public void InputName()
		{

            Console.WriteLine("참치 사냥을 떠나는 것이에요\n");
            Console.WriteLine("원하시는 이름을 설정해 주세요.\n");

            string userName = Console.ReadLine(); // 사용자가 설정하려는 이름을 입력받는 부분

			Console.Clear();
			Console.WriteLine($"당신의 이름은 {userName}입니다.");
            Console.WriteLine("1. 입장 하기 \n2.이름 변경");


            int optionNum = 2;
            int input = GameManager.GM.SelectOption(optionNum, false, "");
            switch (input)
            {
                case 1:
                    entrance.EntranceUI(); // 현재는 시작 화면과 연결되지만 직업 선택으로 연결되게 변경 필요 
                    break;

                case 2:
                    ChangeName(); // 입력한 이름을 변경할 때 이동 
                    break;
            }
        }

        private void ChangeName() // 이름을 재설정 할 때. 
        {
            Console.WriteLine("참치 사냥을 떠나는 것이에요\n");
            Console.WriteLine("변경 할 이름을 설정해 주세요.\n");

            string userName = Console.ReadLine();

            Console.Clear();
            Console.WriteLine($"당신의 이름은 {userName}입니다.");
            Console.WriteLine("1. 입장 하기 \n2.이름 변경");


            int optionNum = 2;
            int input = GameManager.GM.SelectOption(optionNum, false, "");
            switch (input)
            {
                case 1:
                    entrance.EntranceUI();
                    break;

                case 2:
                    ChangeName();
                    break;
            }
        }


    }

	
}
