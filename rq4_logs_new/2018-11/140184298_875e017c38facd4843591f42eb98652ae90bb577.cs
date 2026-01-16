using UnityEngine;

public class AudioController : MonoBehaviour {
    #region Inspector Variables
    [SerializeField] private Source _Source;
    [SerializeField] private float _MaxVelocity;
    [SerializeField] private float gain = 0.5F;
    #endregion Inspector Variables

    #region Private Variables
    private Transform _SourceTransform;
    private bool running = false;
    private int _SampleRate;

    private int _FirstSample;
    private int _PrevSampleVelocity;
    private double _PrevTime;
    #endregion Private Variables

    #region Unity Methods
    private void Start()
    {
        
        _SourceTransform = _Source.transform;

        _SampleRate = AudioSettings.outputSampleRate;
        _PrevSampleVelocity = 0;

        var distFromSource = Vector3.Distance(transform.position, _SourceTransform.position);
        _FirstSample = (int)(distFromSource / _MaxVelocity * _SampleRate);

        running = true;
    }

    private void OnAudioFilterRead(float[] data, int channels)
    {
        //pause if the application is not running
        if (!running) { return; }

        //current distance from the source
        var distFromSource = Vector3.Distance(transform.position, _SourceTransform.position);

        //index of the sample from source clip, that corresponds to the current position
        var lastSample = (int)(distFromSource / _MaxVelocity * _SampleRate);
        var deltaSample = lastSample - _FirstSample;

        //a fragment of source clip based on distance travelled since last update
        var fragment = _Source.GetClipFragment(_FirstSample, lastSample);

        //time since the last updare mesaured in samples
        var sampleTime = data.Length / channels;
        var timeSquared = sampleTime * sampleTime;

        //approximate acceleration based on distance travelled since last update
        //acceleration is constant
        var sampleAcceleration = 2 * (deltaSample - _PrevSampleVelocity * sampleTime) / (float)timeSquared;

        //for each "output" sample
        for (var n = 0; n < sampleTime; n++)
        {
            float value;

            //index of a corresponding sample from original clip fragment
            //can be non-integer
            var realSample = n * (_PrevSampleVelocity + sampleAcceleration * n);
            var idx = (int)realSample;

            //if we go over clip fragment length
            if(idx + 2 >= sampleTime)
            {
                //last sample of the clip fragment
                value = fragment[sampleTime - 1];
            }
            else
            {
                var factor = realSample - idx;
                //linear interpolation based on the two nearest samples from clip fragment
                value = fragment[idx] * (1 - factor) + fragment[idx + 1] * factor;
            }

            for (var i = 0; i < channels; i++)
            {
                data[n * channels + i] = gain * value;
            }

        }

        _FirstSample = lastSample;
        _PrevSampleVelocity += (int)(sampleTime * sampleAcceleration);
    }
    #endregion Unity Methods
}