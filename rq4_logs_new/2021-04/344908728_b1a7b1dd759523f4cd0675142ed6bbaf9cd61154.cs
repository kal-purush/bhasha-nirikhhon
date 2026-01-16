using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class RadialClockController : MonoBehaviour
{

    public List<StationInstance> stations;
    public GameObject radialClockPrefab;

    List<Image> clocks;

    Vector3 offset;

    // Start is called before the first frame update
    void Start()
    {
        offset = new Vector3(0.0f, 30.0f, 0.0f);
        clocks = new List<Image>();
        foreach(StationInstance station in stations)
        {
            GameObject clock = Instantiate(radialClockPrefab);
            clock.transform.SetParent(transform);
            Image clockImage = clock.GetComponent<Image>();
            clockImage.fillAmount = 0;
            clock.transform.position = Camera.main.WorldToScreenPoint(station.transform.position) + offset;
            clocks.Add(clockImage);
        }
    }

    // Update is called once per frame
    void Update()
    {
    }

    public void startClock(StationInstance station, float time)
    {
        StartCoroutine(LerpClock(clocks[stations.IndexOf(station)], time));
    }

    private IEnumerator LerpClock(Image clock, float time)
    {
        for (float f = 0; f <= time; f += Time.deltaTime)
        {
            clock.fillAmount = Mathf.Lerp(0, 1, f / time);
            yield return null;
        }

        clock.fillAmount = 0;
    }
}