using UnityEngine;
using System.Collections;

/// <summary>
/// Author: Matt Gipson
/// Contact: Deadwynn@gmail.com
/// Domain: www.livingvalkyrie.com
/// 
/// Description: DestroySelfAfterWaitTime 
/// </summary>
public class DestroySelfAfterWaitTime : MonoBehaviour {
    #region Fields

    [Tooltip("the amount of time before object destorys itself")]
    public float waitTime = 0.5f;

    #endregion

    void Start() {
        StartCoroutine("CountDown");
    }

    IEnumerator CountDown() {
        yield return new WaitForSeconds(waitTime);
        Destroy(this.gameObject);
    }

}