
public interface IAnimation
{
    string Animation(int speed); //type is subject to change
}

public class TopSpin : IAnimation
{
    public string Animation(int speed)
    {
        string msg = "Top Spin animation at : " + speed;
        return msg;
    }
}

public class BackSpin : IAnimation
{
    public string Animation(int speed)
    {
        return "Back Spin animation at : " + speed;
    }
}

public class RightSpin : IAnimation
{
    public string Animation(int speed)
    {
        return "Right Spin animation at : " + speed;
    }
}

public class LeftSpin : IAnimation
{
    public string Animation(int speed)
    {
        return "Left Spin animation at : " + speed;
    }
}