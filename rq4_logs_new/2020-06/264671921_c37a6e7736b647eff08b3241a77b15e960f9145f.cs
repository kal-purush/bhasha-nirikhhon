using TMPro;
using UnityEngine;

public class InfoBoard : MonoBehaviour
{
    public int calendarCurrentDate = 2007;
    public int calendarMinDate = 2007;
    public int calendarMaxDate = 2017;
    public TextMeshPro calendarDateText;

    public void CalendarDateDown()
    {
        if (calendarCurrentDate - 1 >= calendarMinDate)
            calendarCurrentDate -= 1;
        else
            return;

        // filter data

        calendarDateText.text = calendarCurrentDate.ToString();
    }

    public void CalendarDateUp()
    {
        if (calendarCurrentDate + 1 <= calendarMaxDate)
            calendarCurrentDate += 1;
        else
            return;

        // filter data

        calendarDateText.text = calendarCurrentDate.ToString();
    }
}