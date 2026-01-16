using UnityEngine;
using System.Collections;

public class GhostAnimatorController : MonoBehaviour
{
	private Animator ghostAnimator;
	private CharacterController controller;

	void Start()
	{
		this.controller = this.GetComponentInParent<CharacterController>();
		this.ghostAnimator = this.GetComponentInChildren<Animator>();
	}

	void Update()
	{
		if (this.controller.isGrounded && this.controller.velocity != Vector3.zero)
		{
			this.ghostAnimator.SetBool("walking", true);
		}
		else
		{
			this.ghostAnimator.SetBool("walking", false);
		}
	}
}