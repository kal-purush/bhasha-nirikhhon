using UnityEngine;
using System.Collections;

public class costamScript : MonoBehaviour {
    
    public bool isGrowing = true;
    public bool ended = false;

    // Use this for initialization
    void Start () {
        this.gameObject.transform.localScale = Vector3.one * 0.1f;
    }
	
	// Update is called once per frame
	void Update ()
    {
        if (this.gameObject.transform.localScale.x < 1 && isGrowing && !ended)
        {
            this.gameObject.transform.localScale += Vector3.one * 0.1f;
        }
        else if (!ended)
        {
            ended = true;
            StartCoroutine(destroyMe());
        }

        if (this.gameObject.transform.localScale.x > 0 && !isGrowing)
        {
            this.gameObject.transform.localScale -= Vector3.one * 0.1f;
        }
        else if (this.gameObject.transform.localScale.x < 0.1f && !isGrowing)
        {
            Destroy(this.gameObject);
        }
    }

    IEnumerator destroyMe()
    {
        yield return new WaitForSeconds(1);
        isGrowing = false;
    }
}