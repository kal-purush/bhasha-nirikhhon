using UnityEngine;
using UnityEngine.UI;

public class LiveUI : MonoBehaviour
{

    public Text livesText;

    // Update is called once per frame
    void Update()
    {
        livesText.text = (EndLocation.count).ToString() + " LIVES";
    }
}
