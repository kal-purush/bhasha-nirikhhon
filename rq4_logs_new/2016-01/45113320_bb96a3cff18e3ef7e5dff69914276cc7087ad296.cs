using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class PlayerAim : MonoBehaviour {
    [Header("Cursor")]
    public float cursorSensibility;
    public SpriteRenderer cursor;
    private Vector3 cursorPosition;
    int playerId;
	// Use this for initialization
	void Awake () {
        cursorPosition = Camera.main.WorldToScreenPoint(transform.position);
        cursorPosition.y = Screen.height / 2f;
        cursorSensibility = Screen.width / 3f;
        cursor = GameObject.Find("cursor"+GetComponent<Player>().playerId).GetComponent<SpriteRenderer>();
        playerId = GetComponent<Player>().playerId;
	}

    
    // Update is called once per frame
    void Update () {
        float h = Input.GetAxisRaw("Horizontal " + playerId);
        float v = Input.GetAxisRaw("Vertical " + playerId);
        if (Mathf.Abs(h) > 0.6f)
            cursorPosition.x += h * cursorSensibility * Time.deltaTime;
        if (Mathf.Abs(v) > 0.6f)
            cursorPosition.y -= v * cursorSensibility * Time.deltaTime;
        Vector3 newPos = Camera.main.ScreenToWorldPoint(cursorPosition);
        newPos.z = 0;
        cursor.gameObject.transform.position = newPos;

    }

    public Vector3 getCursorPosition()
    {
        return cursorPosition;
    }
}