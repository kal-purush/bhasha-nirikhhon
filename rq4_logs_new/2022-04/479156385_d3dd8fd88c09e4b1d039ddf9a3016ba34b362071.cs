using UnityEngine;
using UnityEngine.UI;

public class FeedButtonController : MonoBehaviour
{
    int health = 10;
    public Text textField;
    private float timer = 0.0f;

    public void FeedEevee ()
    {
        health = health + 1;
        textField.text = health.ToString();
        timer = 0;
    }

    public void Update ()
    {
        timer += Time.deltaTime;
        if (timer > 3)
            if (health > 0)
            {
                health = health - 1;
                textField.text = health.ToString();
                timer = 0;
            }
    }

}