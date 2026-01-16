using UnityEngine;
using System.Collections;

public class CheatKeyObserver : MonoBehaviour
{
	
	// Update is called once per frame
	void Update ()
    {
	    if(Input.GetKeyDown(KeyCode.UpArrow))
        {
            Date date = GameManager.Instance.GameDate;
            date.Day += 1;
            GameManager.Instance.GameDate = date;
        }

        if(Input.GetKeyDown(KeyCode.A))
        {
            AutoSchedule();
        }
	}

    private void AutoSchedule()
    {
        for(int i = 1; i <= 24; i += 1)
        {
            if (i <= 12)
                SchedulingManager.Instance.SetSchedule(i, ScheduleType.TakeARest);
            else
                SchedulingManager.Instance.SetSchedule(i, ScheduleType.BasicMath);
        }
    }
}