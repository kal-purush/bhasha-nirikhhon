
using UnityEngine;

public class boxerInput : MonoBehaviour
{
  private Animator anim;

  void Start()
  {
    anim = GetComponent<Animator>();
  }
  void Update()
  {
    // ポーズ中じゃなければ実行
    if (!pauseManager.getPause())
    {
      anim.SetBool("LeftPunch", false);
      anim.SetBool("RightPunch", false);
      anim.SetBool("Guard", false);
      anim.SetBool("Counter", false);
      anim.SetBool("Down", false);
      anim.SetBool("Reborn", false);

      // 左パンチ
      if (inputManager.GetDownLeft1()
        && !inputManager.GetDownRight1()
        && !boxerState.GetBoxerState2().IsName("Base Layer.Guard"))
      {
        anim.SetBool("LeftPunch", true);
      }
      // 右パンチ
      if (inputManager.GetDownRight1()
        && !inputManager.GetDownLeft1()
        && !boxerState.GetBoxerState2().IsName("Base Layer.Guard"))
      {
        anim.SetBool("RightPunch", true);
      }
      // ガード
      if (inputManager.GetDownLeft1()
        && inputManager.GetDownRight1())
      {
        anim.SetBool("Guard", true);
      }
      // カウンター
      if (inputManager.GetDownRight1()
        || inputManager.GetDownLeft1())
      {
        if (boxerState.GetBoxerState2().IsName("Base Layer.Guard"))
        {
          anim.SetBool("Counter", true);
        }
      }
      // ダウン
      if (hpManager.hp1 <= 0)
      {
                anim.SetBool("Down", true);
              //  anim.SetTrigger("Down");
      }
      // リターン
      if (countManager.Count >= 10
        && rebornManager.Count != 0
        && !boxerState.GetBoxerState1().IsName("Base Layer.Reborn"))
      {
        Debug.Log("12gubhjmhgfrghj3");
        anim.SetBool("Reborn", true);
      }
    }
  }
}