using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class TutorialSceneManager : MonoBehaviour {

    public Image myImageComponent;
    public Sprite Imag1;
    public Sprite Imag2;
    public Sprite Imag3;
    public Sprite Imag4;
    public Sprite Imag5;
    public Sprite Imag6;
    public Sprite Imag7;
    public Sprite Imag8;

    int a = 1;

    // Use this for initialization
    void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}

    private void SetImage1() 
    {
        myImageComponent.sprite = Imag1;
    }
    private void SetImage2() 
    {
        myImageComponent.sprite = Imag2;
    }
    private void SetImage3() 
    {
        myImageComponent.sprite = Imag3;
    }
    private void SetImage4()
    {
        myImageComponent.sprite = Imag4;
    }
    private void SetImage5() 
    {
        myImageComponent.sprite = Imag5;
    }
    private void SetImage6()
    {
        myImageComponent.sprite = Imag6;
    }
    private void SetImage7()
    {
        myImageComponent.sprite = Imag7;
    }
    private void SetImage8()
    {
        myImageComponent.sprite = Imag8;
        MoveToWorldScene();
    }

    public void MoveToWorldScene()
    {
        SceneTransitionManager.Instance.GoToScene(PocketDroidsConstants.SCENE_WORLD, new List<GameObject>());
    }

    public void OnClick(Button button)
    {
        switch (a) {
            case 1:
                a++;
                SetImage1();
                break;
            case 2:
                a++;
                SetImage2();
                break;
            case 3:
                a++;
                SetImage3();
                break;
            case 4:
                a++;
                SetImage4();
                break;
            case 5:
                a++;
                SetImage5();
                break;
            case 6:
                a++;
                SetImage6();
                break;
            case 7:
                a++;
                SetImage7();
                break;
            case 8:
                a=1;
                SetImage8();
                break;
        }
    }
}