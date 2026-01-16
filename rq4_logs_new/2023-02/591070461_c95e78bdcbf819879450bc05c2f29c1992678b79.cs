using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using TMPro;

public class HeartController : MonoBehaviour
{
    static int hearts = 3;
    [SerializeField] private TMP_Text heartText;
    void Awake()
    {
        UpdateHearts();
        heartText = GetComponent<TMP_Text>();
    }
    public void TakeDamage()
    {
        hearts--;
        UpdateHearts();
        if (hearts == 0)
        {
            hearts = 3;
            SceneManager.LoadScene(SceneManager.GetActiveScene().buildIndex);
        }
    }
    void UpdateHearts()
    {
        heartText.text = "Hearts : " + hearts;
    }
}