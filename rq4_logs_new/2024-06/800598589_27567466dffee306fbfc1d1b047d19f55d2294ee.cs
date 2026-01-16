
using Godot;

public static class Math
{
    public static float RadToGodot(float radians)
    {
        float angle = ClampRad(radians);
        return angle > Mathf.Pi ? 2*Mathf.Pi - angle : -angle;
    }

    public static float GodotToRad(float godotAngle)
    {
        return -godotAngle < 0.0f ? 2*Mathf.Pi - godotAngle : -godotAngle;
    }

    public static float ClampRad(float radians)
    {
        float angle = radians % (2*Mathf.Pi);
        return angle < 0.0f ? 2*Mathf.Pi + angle : angle;
    }
}