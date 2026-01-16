using UnityEditor;
using UnityEngine;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

// Containing list time and vector of object
public class Object
{
    public List<double> timeList;
    public List<Vector3> vectorList;
}

// Containing sequence time and id of object
public class MovingSequence
{
    public double time;
    public int id;
}

public class InitializeObject : MonoBehaviour {
    
    // Check if user press any key for beginning to move objects
    private bool flag;

    private int id;
    private float x, y, z;
    private double time;
    
    private List<Object> objects;
    private List<GameObject> gameObjects;
    private List<MovingSequence> sequence;

    // Index of sequence array
    private int index;
    // Time of moving object
    private double endTime;
    // Check if object is moving
    private bool isMoving;

    // Use this for initialization
    void Start()
    {
        objects = new List<Object>();
        sequence = new List<MovingSequence>();
        gameObjects = new List<GameObject>();
        x = 0;
        y = 0;
        z = 0;
        
        index = 1;
        endTime = 0;
        isMoving = false;
        flag = false;

        // Reading data and initialize data of array
        readFile();

        for (int i = 0; i < objects.Count; i++)
        {
            // Initialize prefab of cube (clone objects)
            GameObject tmp = Instantiate(Resources.Load("Prefabs")) as GameObject;

            // Adding script ObjectController for instantiated object
            tmp.AddComponent<ObjectController>();

            // Setting valuable of script ObjectController
            tmp.GetComponent<ObjectController>().vectorList = new List<Vector3>();
            tmp.GetComponent<ObjectController>().vectorList = objects[i].vectorList;
            tmp.GetComponent<ObjectController>().timeList = new List<double>();
            tmp.GetComponent<ObjectController>().timeList = objects[i].timeList;
            
            tmp.transform.position = objects[i].vectorList[0];
            tmp.SetActive(true);
            
            gameObjects.Add(tmp);
        }
	}
	
	// Update is called once per frame
	void Update()
    {
        // If user press any key => Starting program
        if (Input.anyKey)
            flag = true;
        
        if(flag && index < sequence.Count)
        {
            // If object is moving (current time = Time.time < endTime)
            if(isMoving && Time.time > endTime)
            {
                // Set flag = true to object can move
                gameObjects[sequence[index].id - 1].GetComponent<ObjectController>().flag = true;
                index++;
                isMoving = false;
            }
            else if (!isMoving)
            {
                endTime = Time.time + sequence[index].time - sequence[index - 1].time;
                isMoving = true;
            }
        }
	}

    void readFile()
    {
        string data = @"object_id1 x=3 y=10 z=5 17:05:59
object_id1 x=5 y=10 z=5 17:06:00
object_id1 x=7 y=10 z=5 17:06:01
object_id1 x=9 y=10 z=5 17:06:02
object_id1 x=9 y=7 z=5 17:06:03
object_id1 x=9 y=5 z=5 17:06:04
object_id1 x=9 y=3 z=5 17:06:05
object_id1 x=9 y=1 z=5 17:06:06
object_id1 x=7 y=1 z=5 17:06:07
object_id1 x=5 y=1 z=5 17:06:08
object_id1 x=3 y=1 z=5 17:06:09
object_id1 x=3 y=3 z=5 17:06:10
object_id1 x=3 y=5 z=5 17:06:11
object_id1 x=3 y=7 z=5 17:06:12
object_id1 x=3 y=10 z=5 17:06:13
object_id2 x=10 y=3 z=5 17:06:14
object_id2 x=10 y=5 z=5 17:06:15
object_id1 x=9 y=10 z=10 17:06:15
object_id1 x=9 y=7 z=10 17:06:17
object_id3 x=11 y=3 z=10 17:06:19
object_id3 x=11 y=5 z=10 17:06:20
object_id1 x=9 y=5 z=10 17:06:21
object_id2 x=10 y=7 z=5 17:06:22
object_id2 x=10 y=10 z=5 17:06:23
object_id1 x=9 y=3 z=10 17:06:24
object_id1 x=9 y=1 z=10 17:06:25
object_id3 x=11 y=7 z=10 17:06:26
object_id3 x=11 y=1 z=10 17:06:27";
        string tmp = null;
        // Check for setting data
        // 1 = setting for id
        // 2 = x
        // 3 = y
        // 4 = z
        // 5 = time
        // 6 = initialize array or adding valuable for array
        int valFlag = 1;
        // Index of array setting valuable
        // -1 if array isn't containing valuable of id
        int idx = -1;
        
        for (int i = 0; i < data.Length; i++)
        {
            if(data[i] != ' ' && data[i] != '\r' && data[i] != '\n')
            {
                tmp += data[i];
            }
            else
            {
                // Set value of time, x, y, z, id, array
                setValues(tmp, ref valFlag, ref idx);
                tmp = null;
            }
        }
        
        // Because loop out before setting completedly
        // Set value of time
        setValues(tmp, ref valFlag, ref idx);
        // Set value of array
        setValues(tmp, ref valFlag, ref idx);
    }

    void setValues(string value, ref int valFlag, ref int idx)
    {
        switch (valFlag)
        {
            case 1:
                // Cut string and set value of id
                id = int.Parse(value.Substring(value.Length - 1));
                idx = checkID(id);
                break;
            case 2:
                // Cut string and parse float for x
                x = float.Parse(value.Substring(2));
                break;
            case 3:
                y = float.Parse(value.Substring(2));
                break;
            case 4:
                z = float.Parse(value.Substring(2));
                break;
            case 5:
                // Convert string to seconds
                time = TimeSpan.Parse(value).TotalSeconds;
                break;
            default:
                valFlag = 0;

                MovingSequence movsequence = new MovingSequence();
                movsequence.id = id;
                movsequence.time = time;

                // If array isn't containg id
                if (idx == -1)
                {
                    // Initiatlize
                    Object objct = new Object();
                    objct.vectorList = new List<Vector3>();
                    objct.timeList = new List<double>();

                    objct.timeList.Add(time);
                    objct.vectorList.Add(new Vector3(x, y, z));

                    objects.Add(objct);
                }
                else
                {
                    // Add valuable for array if id contains in array
                    objects[sequence[idx].id - 1].timeList.Add(time);
                    objects[sequence[idx].id - 1].vectorList.Add(new Vector3(x, y, z));
                }

                sequence.Add(movsequence);

                break;
        }
        valFlag++;
    }

    // Return index of array where id (which input) compare with id of sequence
    // -1 if array doesn't contain id
    int checkID(int id)
    {
        for(int i = sequence.Count - 1; i >= 0; i--)
        {
            if (id == sequence[i].id)
                return i;
        }
        return -1;
    }
}