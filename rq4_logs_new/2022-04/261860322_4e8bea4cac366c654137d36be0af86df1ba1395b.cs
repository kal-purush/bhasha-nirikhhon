using SpringChallenge2022;

/**
 * Auto-generated code below aims at helping you parse
 * the standard input according to the problem statement.
 **/
public class Entity
{
    public int Id;
    public EntityType Type;
    public int X, Y;
    public int ShieldLife;
    public int IsControlled;
    public int Health;
    public int Vx, Vy;
    public bool TargetingBase;
    public int ThreatFor;

    public Entity(
        int id,
        EntityType type,
        int x,
        int y,
        int shieldLife,
        int isControlled,
        int health,
        int vx,
        int vy,
        bool targetingBase,
        int threatFor)
    {
        Id = id;
        Type = type;
        X = x;
        Y = y;
        ShieldLife = shieldLife;
        IsControlled = isControlled;
        Health = health;
        Vx = vx;
        Vy = vy;
        TargetingBase = targetingBase;
        ThreatFor = threatFor;
    }
}