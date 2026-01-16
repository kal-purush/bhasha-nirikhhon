using UnityEngine;

public class BallController : MonoBehaviour
{
    public PaddleController paddle;
    private Vector3 paddleToBallVector;
    bool gameStarted = false;
	// Use this for initialization
	void Start ()
    {
        paddleToBallVector = this.transform.position - paddle.transform.position;
	}
	
	// Update is called once per frame
	void Update ()
    {
        if(!gameStarted)
        {
            this.transform.position = paddle.transform.position + paddleToBallVector;

            if (Input.GetMouseButtonDown(0))
            {
                print("Mouse Clicked, launching ball");
                this.GetComponent<Rigidbody2D>().velocity = new Vector2(2f, 10f);
                gameStarted = true;
            }
        }
	}
}