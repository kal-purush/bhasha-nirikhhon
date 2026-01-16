using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Rendering.PostProcessing;

public class CameraShift : MonoBehaviour
{
    // Start is called before the first frame update

    PostProcessVolume m_Volume;
    ChromaticAberration m_ChromaticAberration;
    public float shift;


    void Start()
    {
        m_Volume = GetComponent<PostProcessVolume>();
        m_Volume.profile.TryGetSettings(out m_ChromaticAberration);

    }

    // Update is called once per frame
    void Update()
    {
        //this is probably an inefficient way to do this, but it works for now
        //I kind of want to scale it exponentially, but this is a simplier way to implement it for now
        shift = GameManager.instance.stopwatch/GameManager.instance.timer;

        m_ChromaticAberration.intensity.value = 1f * shift;

    }
}