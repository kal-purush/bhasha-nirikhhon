using UnityEngine;
using System.Collections;

public class Arms : MonoBehaviour
{


    public bool oneArm;
    public bool twoArms;
    public bool collideWithArm;
    public float armCount = 0.0f;
    public bool nearArm;
    public bool pressedOnce;
    public SpriteRenderer sprRend;
    public Sprite oneArmSpr;
    public Sprite twoArmsSpr;
    public Sprite oneArmOneLeg;
    public Sprite oneArmTwoLegs;
    public Sprite twoArmsOneLeg;
    public Sprite twoArmsTwoLegs;
    public Sprite torsoOnly;
    public Sprite oneLeg;
    public Sprite twoLegs;
    public GameObject[] goWithTag;
    public GameObject[] allArms;
    public GameObject armObj;
    Torso body;
    LegObject leg;


    Transform player;
    Vector3 pos;

    // Use this for initialization
    void Start()
    {
        //body = new Torso ();
        twoArms = false;
        oneArm = false;
        leg = gameObject.GetComponent<LegObject>();
        body = gameObject.GetComponent<Torso>();
        player = GameObject.Find("ROLLINGHEAD_0").transform;
        sprRend = GetComponent<SpriteRenderer>();
        armCount = 0;
        goWithTag = new GameObject[100];
    }

    // Update is called once per frame
    void Update()
    {
        attachOrDetachArms();
        nearArm = CheckCloseToTag("arm", 0.5f);
        //hasTorso = body.hasTorso;
        //detachArms ();
    }
    //Attaching the arms

    void attachOrDetachArms()
    {
        //Debug.Log ("testing in here"); 
        //armCount = 0;
        Debug.Log("Arm Count is " + armCount);
        if (armCount == 0 && Input.GetKeyDown(KeyCode.Alpha2))
        {
            Debug.Log("Cannot detach! You don't have any arms!");
        }

        if (!body.hasTorso && nearArm && Input.GetKeyDown(KeyCode.X))
        {
            Debug.Log("You need a torso to attach the arm!");
        }
        else if (Input.GetKeyDown(KeyCode.X) && nearArm)
        {
            attach();

        }
        if (Input.GetKeyDown(KeyCode.Alpha2))
        {
            detach();
        }
        changeSprites();
    }

    //Drawing the different sprites according to the different parts
    void changeSprites()
    {
        //no arm no leg
        if (armCount == 0 && leg.legCount == 0 && body.hasTorso)
        {
            sprRend.sprite = torsoOnly;
        }
        //no arm, one leg
        if (armCount == 0 && leg.legCount == 1)
        {
            sprRend.sprite = oneLeg;
        }
        //no arm, two legs
        if (armCount == 0 && leg.legCount == 2)
        {
            sprRend.sprite = twoLegs;
        }
        //only one arm
        if (armCount == 1 && leg.legCount == 0)
        {
            sprRend.sprite = oneArmSpr;
        }
        //one arm and one leg
        if (armCount == 1 && leg.legCount == 1)
        {
            sprRend.sprite = oneArmOneLeg;
        }
        //one arm and two legs
        if (armCount == 1 && leg.legCount == 2)
        {
            sprRend.sprite = oneArmTwoLegs;
        }
        //Only two arms
        if (armCount == 2 && leg.legCount == 0)
        {
            sprRend.sprite = twoArmsSpr;
        }
        //two arms and one leg
        if (armCount == 2 && leg.legCount == 1)
        {
            sprRend.sprite = twoArmsOneLeg;
        }
        //two arms and two legs
        if (armCount == 2 && leg.legCount == 2)
        {
            sprRend.sprite = twoArmsTwoLegs;
        }
    }

    //Checking if the player is near the torso body part
    bool CheckCloseToTag(string tag, float minimumDistance)
    {
        allArms = GameObject.FindGameObjectsWithTag("arm");

        for (int i = 0; i < allArms.Length; ++i)
        {
            if (Vector3.Distance(transform.position, allArms[i].transform.position) <= minimumDistance)
            {
                armObj = allArms[i];
                goWithTag[i] = allArms[i];
                Debug.Log("Press X to pick up");
                return true;
            }
        }
        return false;
    }

    void CreateArmObj()
    {
        pos = player.position;
        armObj.SetActive(true);
        Instantiate(armObj, pos, Quaternion.Euler(0f, 0f, 0f));
        Destroy(armObj);
    }

    /* 
	 * 
	 * The different conditions for attach/detach parts
	 * 
	 * */

    void attach()
    {
        if (Input.GetKeyDown(KeyCode.X) && nearArm && body.hasTorso && armCount == 0 && !leg.twoLegs)
        {
            //change sprite to head with torso and one arm
            //any special abilities go here.
            Debug.Log("In here 1");
            armCount = 1;
            oneArm = true;
            armObj.SetActive(false);
            pressedOnce = true;
        }
        //Attaching the arm leaves with a head, torso, and two arms. 
        else if (Input.GetKeyDown(KeyCode.X) && nearArm && pressedOnce && body.hasTorso && armCount == 1 && !leg.twoLegs)
        {
            oneArm = false;
            twoArms = true;
            armObj.SetActive(false);
            armCount = 2;
            Debug.Log("In here 2");
            //sprRend.sprite = twoArmsSpr;
            //change sprite to head with torso and two arms
        }
        else if (Input.GetKeyDown(KeyCode.X) && nearArm && body.hasTorso && armCount == 1 && leg.oneLeg && !leg.twoLegs)
        {
            //change sprite to head with torso, two arms and 1 leg
            oneArm = false;
            twoArms = true;
            armObj.SetActive(false);
            armCount = 2;
            Debug.Log("In here 3");
        }
        else if (Input.GetKeyDown(KeyCode.X) && nearArm && body.hasTorso && armCount == 1 && !leg.oneLeg && leg.twoLegs)
        {
            //change sprite to head with torso, two arms and 2 legs
            oneArm = false;
            twoArms = true;
            armObj.SetActive(false);
            armCount = 2;
            Debug.Log("In here 4");
        }
        else if (Input.GetKeyDown(KeyCode.X) && nearArm && body.hasTorso && armCount == 0 && leg.oneLeg && !leg.twoLegs)
        {
            //change sprite to head with torso, one arm and 1 legs
            oneArm = true;
            twoArms = false;
            armObj.SetActive(false);
            armCount = 1;
            Debug.Log("In here 5");
        }
        else if (Input.GetKeyDown(KeyCode.X) && nearArm && body.hasTorso && armCount == 0 && !leg.oneLeg && leg.twoLegs)
        {
            //change sprite to head with torso, 1 arms and 2 legs
            oneArm = true;
            twoArms = false;
            armObj.SetActive(false);
            armCount = 1;
            Debug.Log("In here 6");
        }
    }


    void detach()
    {
        if (Input.GetKeyDown(KeyCode.Alpha2) && body.hasTorso && armCount == 1 && leg.legCount == 0)
        {
            //change sprite to head with torso
            armCount = 0;
            oneArm = false;
            CreateArmObj();
            //createObjArm();
        }
        else if (Input.GetKeyDown(KeyCode.Alpha2) && body.hasTorso && armCount == 2 && leg.legCount == 0)
        {
            //change sprite to head with torso and one arm
            armCount = 1;
            oneArm = true;
            twoArms = false;
            CreateArmObj();
            //createObjArm();
        }
        else if (Input.GetKeyDown(KeyCode.Alpha2) && body.hasTorso && armCount == 2 && leg.legCount == 1)
        {
            //change sprite to head with torso, one arm and one leg
            armCount = 1;
            twoArms = false;
            oneArm = true;
            CreateArmObj();
            //createObjArm ();
        }
        else if (Input.GetKeyDown(KeyCode.Alpha2) && body.hasTorso && armCount == 2 && leg.legCount == 2)
        {
            //change sprite to head with torso, one arm and two legs
            armCount = 1;
            twoArms = false;
            oneArm = true;
            CreateArmObj();
        }
        else if (Input.GetKeyDown(KeyCode.Alpha2) && body.hasTorso && armCount == 1 && leg.legCount == 1)
        {
            //change sprite to head with torso, and one legs
            armCount = 0;
            twoArms = false;
            oneArm = false;
            CreateArmObj();
        }
        else if (Input.GetKeyDown(KeyCode.Alpha2) && body.hasTorso && armCount == 1 && leg.legCount == 2)
        {
            //change sprite to head with torso and two legs
            armCount = 0;
            twoArms = false;
            oneArm = false;
            CreateArmObj();
        }
    }
}