namespace GameOne.Source.Enumerations
{
    [System.Flags]
    public enum CollisionResponse
    {
        Ignore,
        Immovable,
        Project,
        DestroyOnImpact
    }
}