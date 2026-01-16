using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class altCrowdManager : MonoBehaviour
{
    [SerializeField] private Vector2 crowdPresentDuration = new Vector2(5f, 10f);
    [SerializeField] private Vector2 crowdAbsentDuration = new Vector2(10f, 20f);
    [SerializeField] private float crowdMoveSpeed = 2f;
    private Transform[] crowdSprite = new Transform[2];
    private bool crowdAbsent = false;
    [SerializeField]private float timer;
    
    
    // Start is called before the first frame update
    void Start()
    {
        timer = Random.Range(crowdPresentDuration.x, crowdPresentDuration.y);
        crowdSprite[0] = transform.GetChild(0);
        crowdSprite[1] = transform.GetChild(1);
        
    }

    // Update is called once per frame
    void Update()
    {
        if (timer <= 0)
        {
            crowdAbsent = !crowdAbsent;
            if (crowdAbsent)
            {
                timer = Random.Range(crowdPresentDuration.x, crowdPresentDuration.y);
            }
            else timer = Random.Range(crowdAbsentDuration.x, crowdAbsentDuration.y);
        }
        else timer -= Time.deltaTime;

        if (crowdAbsent)
        {
            crowdSprite[0].position =
                Vector3.Lerp(crowdSprite[0].position,
                    new Vector3(-1.3f, crowdSprite[0].position.y, crowdSprite[1].position.z),
                                                                        crowdMoveSpeed * Time.deltaTime);
            crowdSprite[1].position =
                Vector3.Lerp(crowdSprite[1].position,
                    new Vector3(1.3f, crowdSprite[1].position.y, crowdSprite[1].position.z),
                                                                            crowdMoveSpeed * Time.deltaTime);
        }
        else
        {
            crowdSprite[0].position =
                Vector3.Lerp(crowdSprite[0].position,
                    new Vector3(-4, crowdSprite[0].position.y, crowdSprite[0].position.z),
                                                                            crowdMoveSpeed * Time.deltaTime);
            crowdSprite[1].position =
                Vector3.Lerp(crowdSprite[1].position,
                    new Vector3(4, crowdSprite[1].position.y, crowdSprite[1].position.z),
                                                                            crowdMoveSpeed * Time.deltaTime);
        }
    }
}