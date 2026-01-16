using UnityEngine;
using System.Collections;

[RequireComponent(typeof(SpriteRenderer))]
public class iTweenSprite : MonoBehaviour {

    public static void FadeTo(GameObject target, float from, float to, float time) {
        if (target.GetComponent<iTweenSprite>() == null) {
            target.AddComponent<iTweenSprite>();
        }
        iTween.ValueTo(target, iTween.Hash("from", from, "to", to, "time", time, "onupdate", "SetAlpha"));
    }

    public static void FadeTo(GameObject target, Hashtable args) {
        Debug.Assert (args.Contains ("from") && args.Contains ("to") && args.Contains ("time"));
        FadeTo(target, (float) args["from"], (float) args["to"], (float) args["time"]);
    }

    private void SetAlpha(float alpha) {
        Color color = this.GetComponent<SpriteRenderer>().color;
        color.a = alpha;
        this.GetComponent<SpriteRenderer>().color = color;
    }

    public static void ColorTo(GameObject target, Color from, Color to, float time) {
        if (target.GetComponent<iTweenSprite>() == null) {
            target.AddComponent<iTweenSprite>();
        }
        iTween.ValueTo(target, iTween.Hash("from", from, "to", to, "time", time, "onupdate", "SetColor"));
    }

    public static void ColorTo(GameObject target, Hashtable args) {
        Debug.Assert (args.Contains ("from") && args.Contains ("to") && args.Contains ("time"));
        FadeTo(target, (float) args["from"], (float) args["to"], (float) args["time"]);
    }

    private void SetColor(Color color) {
        this.GetComponent<SpriteRenderer> ().color = color;
    }
}