using ComponentReferenceAttributes;
using UnityEngine;
using UnityEngine.Assertions;
using UnityEngine.UI;

/// <summary>
/// A door-wall pair, where map randomization chooses to use one or the other.
///
/// Designed to make the editing experience easier, since the pair share a parent in the hierarchy.
/// </summary>
public class DoorOrWall : MonoBehaviour
{
    [ChildComponent] public DoorOrWall_Door door;
    [ChildComponent] public DoorOrWall_Wall wall;

    private void OnValidate()
    {
        Assert.IsNotNull( door );
        Assert.IsNotNull( wall );
    }

    public void setTo(bool isDoor)
    {
        door.gameObject.SetActive( isDoor );
        wall.gameObject.SetActive( !isDoor );
    }
}