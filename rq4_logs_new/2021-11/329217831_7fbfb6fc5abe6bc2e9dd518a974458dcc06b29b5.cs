using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EventRoad0 : GameEvent
{
    GameObject player;
    GameObject dog;

    bool isDogMoveEnd = false;
    override protected void InitEvent()
    {
        player = MainObject.Instance.player;
        dog = MainObject.Instance.dog;

        
    }
    override public bool Condition()
    {
        return true;
    }
    /// <summary>
    /// 이동
    /// </summary>
    /// <returns></returns>
    override public IEnumerator EventAction()
    {
        StartCoroutine(MoveDog());
        
        
        yield return new WaitUntil(()=>isDogMoveEnd == true);
    }

    public IEnumerator MoveDog()
    {
        dog.GetComponent<TrackPlayer>().enabled = false;
        float dogVelocity = 7f;
        Vector3 startPosition = dog.transform.position;
        Vector3 arrivePosition = transform.Find("EventPosition").position;
        yield return new WaitForSeconds(2.0f);
        FollowTarget.Instance.ChangeCurrentTarget("dog");
        float rate = 0;
        while(rate <= 1)
        {
            rate += Time.deltaTime * dogVelocity/Vector3.Distance(startPosition, arrivePosition);
            dog.transform.position = Vector3.Lerp(startPosition, arrivePosition, rate);
            yield return null;
        }
        
        
        yield return new WaitForSeconds(2.0f);
        isDogMoveEnd = true;
        FollowTarget.Instance.ChangeCurrentTarget("player");
        dog.GetComponent<TrackPlayer>().enabled = true;
        dog.SetActive(false);
        owner.StartStageEvent(1);

    }
}