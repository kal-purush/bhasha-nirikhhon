using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Hashtable = ExitGames.Client.Photon.Hashtable;

public class m_ScoreManager : MonoBehaviour {
    public const string PlayerLocalScoreProp = "Localscore";
    public const string PlayerRemoteScoreProp = "RemoteScore";
}

public static class myScoreUtility
{
    public static void SetScore(this PhotonPlayer player, int _localScore, int _remoteScore)
    {
        Hashtable score = new Hashtable();  // using PUN's implementation of Hashtable
        score[m_ScoreManager.PlayerLocalScoreProp] = _localScore;
        score[m_ScoreManager.PlayerRemoteScoreProp] = _remoteScore;


        player.SetCustomProperties(score);  // this locally sets the score and will sync it in-game asap.

    }

    public static void AddLocalScore(this PhotonPlayer player, int scoreToAddToCurrent)
    {
        int current = player.GetLocalScore();
        //current = current + scoreToAddToCurrent;

        Hashtable score = new Hashtable();  
        score[m_ScoreManager.PlayerLocalScoreProp] = current;

        player.SetCustomProperties(score);  
    }

    public static void AddRemoteScore(this PhotonPlayer player, int scoreToAddToCurrent)
    {
        int current = player.GetRemoteScore();
        //current = current + scoreToAddToCurrent;

        Hashtable score = new Hashtable();  // using PUN's implementation of Hashtable
        score[m_ScoreManager.PlayerRemoteScoreProp] = current;

        player.SetCustomProperties(score);  // this locally sets the score and will sync it in-game asap.
    }

    public static int GetLocalScore(this PhotonPlayer player)
    {
        object score;
        if (player.CustomProperties.TryGetValue(m_ScoreManager.PlayerLocalScoreProp, out score))
        {
            return (int)score;
        }

        return 0;
    }

    public static int GetRemoteScore(this PhotonPlayer player)
    {
        object score;
        if (player.CustomProperties.TryGetValue(m_ScoreManager.PlayerRemoteScoreProp, out score))
        {
            return (int)score;
        }

        return 0;
    }
}