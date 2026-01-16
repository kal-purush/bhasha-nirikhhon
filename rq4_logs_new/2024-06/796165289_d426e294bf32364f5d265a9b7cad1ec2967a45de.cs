using System.Collections;
using System.Threading.Tasks;
using UnityEngine;

public static class CoroutineExtensions
{
    public static Task AsTask(this IEnumerator coroutine, MonoBehaviour owner)
    {
        var tcs = new TaskCompletionSource<bool>();

        owner.StartCoroutine(RunCoroutine(coroutine, tcs));

        return tcs.Task;
    }

    private static IEnumerator RunCoroutine(IEnumerator coroutine, TaskCompletionSource<bool> tcs)
    {
        while (true)
        {
            try
            {
                if (!coroutine.MoveNext())
                {
                    tcs.SetResult(true);
                    yield break;
                }
            }
            catch (System.Exception ex)
            {
                tcs.SetException(ex);
                yield break;
            }
            yield return coroutine.Current;
        }
    }
}