using UnityEngine;
using System.Collections;

public class iTweenExt : MonoBehaviour {
    private System.Type type;

    protected System.Type GetTargetType(GameObject target) {
        if (target.GetComponent<SpriteRenderer>()) {
            return typeof(SpriteRenderer);
        } else if (target.GetComponent<UnityEngine.UI.Text>()) {
            return typeof(UnityEngine.UI.Text);
        } else {
            Debug.LogError("iTweenExt target type doesn't match any components");
            return null;
        }            
    }

    protected static void AddComponent(GameObject target) {
        iTweenExt me = target.GetComponent<iTweenExt>();
        if (me == null) {
            me = target.AddComponent<iTweenExt>();
        }
        me.type = me.GetTargetType(target);
    }

    public static void FadeTo(GameObject target, float from, float to, float time) {
        FadeTo(target, iTween.Hash("from", from, "to", to, "time", time));
    }

    public static void FadeTo(GameObject target, Hashtable args) {
        AddComponent(target); 
        Debug.Assert(args.Contains("from") && args.Contains("to") && args.Contains("time"));
        args.Add("onupdate", "SetAlpha");
        iTween.ValueTo(target, args);
    }

    public static void ColorTo(GameObject target, Color from, Color to, float time) {
        ColorTo(target, iTween.Hash("from", from, "to", to, "time", time));
    }

    public static void ColorTo(GameObject target, Hashtable args) {
        AddComponent(target); 
        Debug.Assert(args.Contains("from") && args.Contains("to") && args.Contains("time"));
        if (target.GetComponent<iTweenExt>() == null) {
            target.AddComponent<iTweenExt>();
        }

        args.Add("onupdate", "SetColor");
        iTween.ValueTo(target, args);
    }

    protected void SetAlpha(float alpha) {
        Color color;
        if (this.type == typeof(SpriteRenderer)) {
            color = this.GetComponent<SpriteRenderer>().color;
            color.a = alpha;
            this.GetComponent<SpriteRenderer>().color = color;
        } else if (this.type == typeof(UnityEngine.UI.Text)) {
            color = this.GetComponent<UnityEngine.UI.Text>().color;
            color.a = alpha;
            this.GetComponent<UnityEngine.UI.Text>().color = color;
        }
    }

    protected void SetColor(Color color) {
        if (this.type == typeof(SpriteRenderer)) {
            this.GetComponent<SpriteRenderer>().color = color;
        } else if (this.type == typeof(UnityEngine.UI.Text)) {
            this.GetComponent<UnityEngine.UI.Text>().color = color;
        }
    }
}