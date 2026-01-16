using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class enviornmentManager : MonoBehaviour {

    public static enviornmentManager instance;
    public List<GameObject> spikes;

    public GameObject fallingSpike;
    public AudioClip spikeFall;
    public float fallChance = 0.1f;

    // Use this for initialization
    void Start () {
        instance = this;
        spikes = new List<GameObject>();
        for(int i = 0; i < transform.childCount; i++) {
            spikes.Add(transform.GetChild(i).gameObject);
        }
    }

    GameObject targetSpike;
	// Update is called once per frame
	public void enviornmentCall() {
        if (targetSpike != null) {
            audioManager.instance.Play(spikeFall, 1f, Random.Range(0.96f, 1.03f));

            GameObject temp = (GameObject)Instantiate(fallingSpike, targetSpike.transform.position, targetSpike.transform.rotation);
            temp.GetComponent<Rigidbody2D>().AddTorque(UnityEngine.Random.value * 30);
            Destroy(targetSpike);

        } else if (UnityEngine.Random.value < fallChance) {
            int index = UnityEngine.Random.Range(0, spikes.Count);
            targetSpike = spikes[index];
            spikes.RemoveAt(index);
            targetSpike.transform.Translate(new Vector2(0.1f, -0.1f));
            targetSpike.transform.Rotate(new Vector3(0, 0, (UnityEngine.Random.value < 0.5f) ? -15 : 15));
        }
    }
}