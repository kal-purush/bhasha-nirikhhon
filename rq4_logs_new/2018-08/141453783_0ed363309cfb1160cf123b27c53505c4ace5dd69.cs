using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;


public class CheckWrong : MonoBehaviour{
    public Info_Next inf;

    public RawImage Wrong2;
    public RawImage Wrong3;

    public RawImage Vehicle1;
    public RawImage Vehicle2;
    public RawImage Vehicle3;


    public bool correct = false;
    public bool wrong2 = false;
    public bool wrong3 = false;

    float w2Time = 0;
    float w3Time = 0;
    // Use this for initialization
    void Start () {
        //RectTransform vh1 = Vehicle1.GetComponent<RectTransform>();

    }
	
	// Update is called once per frame
	void Update () {

        RectTransform vh2 = Vehicle2.GetComponent<RectTransform>();
        RectTransform vh3 = Vehicle3.GetComponent<RectTransform>();

        RectTransform tw2 = Wrong2.GetComponent<RectTransform>();
        RectTransform tw3 = Wrong3.GetComponent<RectTransform>();

        if (inf.game_start){

        tw2.position = new Vector3(vh2.position.x
                                     , vh2.position.y
                                     , vh2.position.z);
        tw3.position = new Vector3(vh3.position.x
                                     , vh3.position.y
                                     , vh3.position.z);
            inf.game_start = false;
        }

        RawImage w2 = Wrong2.GetComponent<RawImage>();
        RawImage w3 = Wrong3.GetComponent<RawImage>();


        if (wrong2){
            w2.enabled = true;
            w2Time += Time.deltaTime;
            while(w2Time >1f){
                w2.enabled = false;
                wrong2 = false;
                w2Time = 0;
            }
        }
        if (wrong3){
            w3.enabled = true;
            w3Time += Time.deltaTime;
            while (w3Time > 1f){
                w3.enabled = false;
                wrong3 = false;
                w3Time = 0;
            }
        }
        

	}
}