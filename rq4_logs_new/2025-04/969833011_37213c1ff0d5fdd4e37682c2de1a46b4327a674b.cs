using System;

namespace SpartaDungeon
{
    public class Player
    {
        // ================================
        // Player 클래스: 플레이어 캐릭터의 상태와 동작을 관리
        // ================================

        // 자동 구현 init-only 속성:
        // 생성자에서만 초기화 가능하며, 이후 수정 불가능
        public string Name { get; init; }      // 캐릭터 이름
        public string Job { get; init; }       // 직업 (전사, 궁수, 마법사 등)
        public int Level { get; init; }        // 레벨
        public int Attack { get; init; }       // 공격력
        public int Defense { get; init; }      // 방어력
        public int MaxHP { get; init; }        // 최대 체력
        public int CurrentHP { get; set; }      // 현재 체력 (변경 가능)
        public int Gold { get; init; }         // 소지 금화
        public int MaxMP { get; init; }        // 최대 마나
        public int CurrentMP { get; set; }      // 현재 마나 (변경 가능)

        // 생성자:
        // 인자로 받은 초기값으로 모든 속성을 설정
        public Player(string name, string job, int level,
                      int atk, int def, int hp, int gold, int mp)
        {
            Name = name;      // 이름 설정
            Job = job;       // 직업 설정
            Level = level;     // 레벨 설정
            Attack = atk;       // 공격력 설정
            Defense = def;       // 방어력 설정
            MaxHP = hp;        // 최대 체력 설정
            CurrentHP = hp;        // 현재 체력 초기화
            Gold = gold;      // 금화 설정
            MaxMP = mp;        // 최대 마나 설정
            CurrentMP = mp;        // 현재 마나 초기화
        }

        // 상태 보기 메서드:
        // 플레이어의 상태 정보를 콘솔에 출력하고, 사용자가 0을 입력하면 종료
        public void ShowStatus()
        {
            while (true)
            {
                Console.Clear();                       // 화면 지우기
                Console.WriteLine("상태 보기\n");  // 제목 출력

                // ToString() 오버라이드 사용: 레벨/이름/직업/공격력/방어력 출력
                Console.WriteLine(this);

                // 체력, 마나, 금화 출력
                Console.WriteLine($"체력 : {CurrentHP}/{MaxHP}");
                Console.WriteLine($"MP : {CurrentMP}/{MaxMP}");
                Console.WriteLine($"Gold : {Gold} G\n");

                Console.WriteLine("0. 나가기\n"); // 나가기 안내
                Console.Write(">> ");              // 입력 프롬프트

                if (Console.ReadLine() == "0")     // 0 입력 시 루프 종료
                    break;

                // 잘못된 입력 처리
                Console.WriteLine("잘못된 입력입니다");
                Console.WriteLine("계속하려면 아무 키나 누르세요...");
                Console.ReadKey();                   // 대기 후 다시 출력
            }
        }

        // ToString() 오버라이드:
        // 객체를 문자열로 표현할 때 호출됨
        // 레벨, 이름, 직업, 공격력, 방어력을 한 줄로 간결하게 반환
        public override string ToString()
            => $"Lv.{Level:D2}  {Name} ({Job})  ATK:{Attack}  DEF:{Defense}";
    }
}