using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ConstructionQueue : MonoBehaviour
{
    [Tooltip("Array of generatable segments")]
    public SegmentController[] segmentsUsed;

    public enum Mode { RANDOMIZE, FROM_FIXED_LIST }
    public Mode SegmentGenerationType { get; set; }

    public SegmentController GetNextSegment()
    {
        switch (SegmentGenerationType)
        {
            case (Mode.RANDOMIZE):
                return GetRandomSegmentFromArray(this.segmentsUsed);
            case (Mode.FROM_FIXED_LIST):
                return null;
            default:
                return null;
        }
    }

    private SegmentController GetRandomSegmentFromArray(SegmentController[] segments)
    {
        int index = Random.Range(0, segments.Length - 1);
        return segments[index];
    }
}