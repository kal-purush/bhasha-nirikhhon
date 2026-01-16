using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Newtonsoft.Json;

public class JsonObjects : MonoBehaviour
{

    private string testJson = @"{
      'joystick': {
      'x':'5',
      'y':'0'
    },
      'a': 'true',
      'b': 'false',
      'x': 'false',
      'y': 'false',
      'start': 'false'
    }";

    private string testJson2 = @"{'name': 'title',
    'ip': '10.0.0.1',
    'controller':{
      'joystick': {
      'x':'5',
      'y':'0'
    },
      'a': 'true',
      'b': 'false',
      'x': 'false',
      'y': 'false',
      'start': 'false'
    }
    }";

    // Start is called before the first frame update
    void Start()
    {
        StartCoroutine(test(testJson, testJson2));
        
    }
    
    IEnumerator test(string a, string b)
    {
        yield return new WaitForSeconds(5);
        Controller c = deserilize<Controller>(a);
        Debug.Log("A" + c.a + " joystick x "  + c.joystick.x);

        User d = deserilize<User>(b);
        Debug.Log("Player name "+ d.name +"A" + d.controller.a + " joystick x " + d.controller.joystick.x);
    }
    // Update is called once per frame
    void Update()
    {
        
    }

    public T deserilize<T>(string message)
    {
        T thing = JsonConvert.DeserializeObject<T>(message);
        return thing;
    }

    public class User
    {
        public string name;
        public string ip;
        public Controller controller;
    }

    public class Controller
    {
        public Joystick joystick;
        public bool a = false;
        public bool b = false;
        public bool x = false;
        public bool y = false;
        public bool start = false;
    }

    public class Joystick
    {
        public float x;
        public float y;
    }

}
