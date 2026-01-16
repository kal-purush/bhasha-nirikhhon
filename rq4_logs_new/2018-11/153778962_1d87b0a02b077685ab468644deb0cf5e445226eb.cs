using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HUDTitle : MonoBehaviour
{
  private void Start()
  {
    gameObject.SetActive(false);
  }

  public void Show()
  {
    gameObject.SetActive(true);
    Animator titleAnimator = GetComponent<Animator>();
    titleAnimator.SetTrigger("Show");
  }

  public void Hide()
  {
    Animator titleAnimator = GetComponent<Animator>();
    titleAnimator.SetTrigger("Hide");
  }

  public void HideFinished()
  {
    gameObject.SetActive(false);
  }
}