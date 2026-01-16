using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
public class InGameUIController : MonoBehaviour
{

    public Canvas canvas;
    public GameObject LeaderboardWindow;
    public GameObject SaveScorePopUp;



    //Gamiarra show
    Animator ssPopUp;
    private void Start()
    {
        // OnGameEnd();
    }

    public void OnGameEnd()
    {
        // Is a Top Score
        if (ScoreSystem.Instance.Score > Leaderboard.Scores[Leaderboard.Scores.Count - 1].value || Leaderboard.Scores.Count < 10)
        {
            ssPopUp = Instantiate(SaveScorePopUp, Vector3.zero, Quaternion.identity, canvas.transform).GetComponent<Animator>();
        }
        else
        {
            Instantiate(LeaderboardWindow, Vector3.zero, Quaternion.identity, canvas.transform);
        }
    }


    private void Update()
    {
        //Gambiarra show
        //popup sendo exibido
        if (ssPopUp != null)
        {
            if (ssPopUp.GetCurrentAnimatorStateInfo(0).IsName("PopUpOut") &&
                ssPopUp.GetCurrentAnimatorStateInfo(0).normalizedTime >= 1.0f)
            {
                Instantiate(LeaderboardWindow, Vector3.zero, Quaternion.identity, canvas.transform);
                ssPopUp = null;
            }
        }
    }


}