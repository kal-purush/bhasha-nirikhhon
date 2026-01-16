using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerMovement : MonoBehaviour
{
    
    public Animator animator;

    [Range(0, 100f)] [SerializeField] private float m_RunSpeed = 40f;
    [Range(0, .3f)] [SerializeField] private float m_MovementSmoothing = .05f;
    
    private bool m_FacingRight = true;
    private Rigidbody2D m_Rigidbody2D;
    private Vector3 m_Velocity = Vector3.zero;

    float horizontal_move = 0f;
    float vertical_move = 0f;

    void Start()
    {
        m_Rigidbody2D = GetComponent<Rigidbody2D>();
    }

    void Update()
    {
        horizontal_move = Input.GetAxisRaw("Horizontal") * m_RunSpeed;
        vertical_move = Input.GetAxisRaw("Vertical") * m_RunSpeed;

        animator.SetFloat("Speed", Mathf.Abs(horizontal_move) + Mathf.Abs(vertical_move));
    }

    void FixedUpdate()
    {
        Move(horizontal_move * Time.fixedDeltaTime, vertical_move * Time.fixedDeltaTime);
    }

    void Move(float hmove, float vmove)
    {
        Vector3 targetVelocity = new Vector2(hmove * 10f, vmove * 10f);
        m_Rigidbody2D.velocity = Vector3.SmoothDamp(m_Rigidbody2D.velocity, targetVelocity, ref m_Velocity, m_MovementSmoothing);

        if (hmove > 0 && !m_FacingRight)
		{
			Flip();
		}
		else if (hmove < 0 && m_FacingRight)
		{
			Flip();
		}
        else if(vmove > 0 && hmove <= 0 && m_FacingRight)
        {
            Flip();
        }
        else if(vmove < 0 && hmove >= 0 && !m_FacingRight)
        {
            Flip();
        }
    }

    private void Flip()
	{
		m_FacingRight = !m_FacingRight;

		Vector3 theScale = transform.localScale;
		theScale.x *= -1;
		transform.localScale = theScale;
	}

}