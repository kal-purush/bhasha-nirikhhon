using ConsoleMenu.Library.Models;

namespace ConsoleMenu.Library.Extensions;
public static class Vector2AddMaxExtension
{
    /// <summary>
    /// Adds the X together. Uses the largest Y. Expands in a horizontal direction.
    /// </summary>
    /// <param name="v1"></param>
    /// <param name="v2"></param>
    /// <returns>Returns a Vector2 with the X together. Uses the largest Y.</returns>
    public static Vector2 AddMax_Horizontal(this Vector2 v1, Vector2 v2)
        => new(v1.X + v2.X, v1.Y > v2.Y ? v1.Y : v2.Y);

    /// <summary>
    /// Uses the largest X. Adds the Y together. Expands in a vertical direction.
    /// </summary>
    /// <param name="v1"></param>
    /// <param name="v2"></param>
    /// <returns>Returns a Vector2 with the largest X. Adds the Y together.</returns>
    public static Vector2 MaxAdd_Vertical(this Vector2 v1, Vector2 v2)
        => new(v1.X > v2.X ? v1.X : v2.X, v1.Y + v2.Y);
}