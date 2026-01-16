using UnityEngine;

public class LureBehaviors : MonoBehaviour {

    public GameObject thing;
    public Rigidbody rb;
    public ThrowLure throwLure;

    private bool lureActive;

    [HideInInspector] public float despawnTime = 15f;

    void Start()
    {
        lureActive = true;
        rb = GetComponent<Rigidbody>();
        throwLure = FindObjectOfType(typeof(ThrowLure)) as ThrowLure;
    }

    void Update()
    {
        if (lureActive == true)
        {
            rb.AddForce((throwLure.calculateBestThrowSpeed(throwLure.throwPoint.transform.position, throwLure.adjustedHit, 1f) * 100f), ForceMode.Force);
            lureActive = false;
        }

        despawnTime -= Time.deltaTime;

        if (despawnTime <= 0)
        {
            Destroy(thing);
        }

    }

}