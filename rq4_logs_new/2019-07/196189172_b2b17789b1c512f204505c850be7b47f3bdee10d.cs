using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ScreenFactoryInvoker : MonoBehaviour
{
    static Queue<VideoDecodeTask> _videoDecodeTasks;

    private void Awake() {
        _videoDecodeTasks = new Queue<VideoDecodeTask>();
    }

    public static void AddCommand(VideoDecodeTask task)
    {
        _videoDecodeTasks.Enqueue(task);
    }

    private void Update()
    {
        if (_videoDecodeTasks.Count > 0)
        {
            _videoDecodeTasks.Dequeue().Run();
        }
    }
}