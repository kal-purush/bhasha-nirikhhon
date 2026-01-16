using UnityEngine;

public class SaveObject : MonoBehaviour
{
    public static SaveObject Instance { get; private set; }
    public System.DateTime TimeStarted;
    public int CurrentScene = -1;
    // Tasks have states (0 = not started, 1 = started, 2 = finished)

    // Elfendorf
    public Vector3 elfendorfPosition = new Vector3();
    public int TaskBrokenBridge = 0;
    public int TaskBoatOpened = 0;
    public int TaskShoesCollected = 0;

    // Ozean
    public Vector3 ozeanPosition = new Vector3();
    public int TaskHoleFixed = 0;

    // Evil Village

    public void Awake()
    {
        if(Instance == null)
        {
            Instance = this;
            DontDestroyOnLoad(gameObject);
        }
        else
        {
            Destroy(gameObject);
        }

        TimeStarted = System.DateTime.Now;
    }

    public void SetValueOf(string propertyName, object value)
    {
        var property = GetType().GetField(propertyName);
        property?.SetValue(this, value);
    }
}