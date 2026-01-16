using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Threading.Tasks;
public class Audio_Manager : MonoBehaviour
{
    public static Audio_Manager instance;
    [SerializeField] private AudioSource source;
    [SerializeField] private AudioClip UI_Select,UI_Click,UI_Click1,Ball_Hit,Ball_Score,Ball_Launch;
    private void Awake() 
    {
        //SINGLETON
        if(instance == null)
        {
            instance = this;
        }
        else
        {
            Destroy(this.gameObject);
        }

        UI_Select = Resources.Load<AudioClip>("UI_Select");
        UI_Click = Resources.Load<AudioClip>("UI_Click");
        UI_Click1 = Resources.Load<AudioClip>("UI_Click01");
        Ball_Hit = Resources.Load<AudioClip>("Ball_Hit");
        Ball_Score = Resources.Load<AudioClip>("Ball_Score");
        Ball_Launch = Resources.Load<AudioClip>("Ball_Launch");
        
    }

    public async void PlaySound(string sound)
    {
        switch(sound)
        {
            case "UI_Select":
                source.PlayOneShot(UI_Select);
                break;
            case "UI_Click":
                source.PlayOneShot(UI_Click);
                break;
            case "UI_Click01":
                source.PlayOneShot(UI_Click1);
                break;
            case "Ball_Hit":
                source.pitch = Random.Range(0.7f,1.3f);
                source.PlayOneShot(Ball_Hit);
                break;
            case "Ball_Score":
                source.PlayOneShot(Ball_Score);
                break;
            case "Ball_Launch":
                source.PlayOneShot(Ball_Launch);
                break;
        }
        await Task.Delay(1000);
        source.pitch = 1;
    }
}