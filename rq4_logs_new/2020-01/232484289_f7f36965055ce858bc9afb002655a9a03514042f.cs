public class Singleton
{
    private static Singleton instance = null;

    public static Singleton Instance
    {
        get
        {
            if (instance == null)
            {
                instance = new Singleton();
            }
            return instance;
        }
    }

    public int range; // 다른 씬이나 다른 스크립트에서 변수 선언을 안해도 쓸 수 있음, 목표걸음
    public int step; // 현재 걸음수

    public bool isWalking = false;//걷는지 안걷는지를 체크

    public int Monster = 0; //ar 화면에서 나올 몬스터를 결정하는 역할을 함

    public bool panel = true; // 사용자가 걸음수를 지정했을땐 패널이 생성되지 않음
}