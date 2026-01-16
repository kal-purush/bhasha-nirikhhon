using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class IntroductionStory : MonoBehaviour
{
    public TextMeshProUGUI introText;
    public ChangeScenes manager;
    public Image blackOut;

    List<string> intro = new List<string>()
    {
        "While our hero, Simon, was but a child, the underworld opened up and demons began to spread throughout the land.",
        "Simon's village was ransacked and burned to ashes - along with his friends and family.",
        "On that dark night, Simon vowed to eliminate every demon he encountered and bring peace back to the world.",
        "Imbued with anger and resentment, Simon's eyes began to radiate a greenish hue to remember the ones he has lost.",
        "Fueled by revenge, during his youth, Simon trained endlessly in order to learn and master the 3 main combat practices - swordplay, archery, and magic.",
        "Once Simon grew older, he became the beacon of hope for all of humanity and embarked on his legendary journey to slay the great Demon General, Nemesis, and his minions.",
        "Based on the determination and feats Simon would achieve throughout his journey, those that encountered him would later grant him the title of 'HELLWALKER'..."
    };


    private void Start()
    {
        StartCoroutine(startIntro());
    }
    IEnumerator startIntro()
    {
        introText.text = "";
        yield return new WaitForSeconds(1.0f);

        foreach(string line in intro)
        {
            foreach(char c in line)
            {
                introText.text += c;
                yield return new WaitForSeconds(0.05f);
                if(c == '.')
                {
                    yield return new WaitForSeconds(0.03f);
                }
            }
            yield return new WaitForSeconds(1.0f);
            introText.text = "";
        }
        SceneManager.LoadScene("Forest");

    }
}