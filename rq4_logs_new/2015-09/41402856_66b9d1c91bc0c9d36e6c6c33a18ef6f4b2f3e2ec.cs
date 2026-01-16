using System.Collections.Generic;
using System.Collections;
using UnityEngine;

public class Enemy2Controller : EnemyController
{
    [Header("Enemy 2 Settings")]
    public float swoopSpeed;
    public GameObject leftSwoop;
    public GameObject rightSwoop;
    public bool AttackPlayer = false;
    private Hashtable tweenPath = new Hashtable();
    private bool moveRight = true;
    private Vector3 pos;
    private Vector3 axis;
    private Transform enemyProjWall;
    private Transform player;
    private bool outOfPlayerRange = false;
    [Header("Path from Top of Screen Settings")]
    public Vector3 _originalPosition;
    private bool gotOriginalPosition = false;
    private List<Vector3> _waypoints;

    private bool _isOnPath = false;
    private float _pathPercentage = 0f;

    private void Start() 
    {
        base.Start();
        leftSwoop = GameObject.FindGameObjectWithTag("enemy2Left");
        rightSwoop = GameObject.FindGameObjectWithTag("enemy2Right");
        _waypoints = new List<Vector3>();
        pos = transform.position;
        axis = transform.right;
        player = GameObject.FindGameObjectWithTag("Player").GetComponent<Transform>();
        enemyProjWall = GameObject.Find("EnemyProjectileWall").GetComponent<Transform>();
        //AttackPlayer = true;
        //SetPath();
	}

    private void Update()
    {
        base.Update();
        if (AttackPlayer)
        {
            if (gotOriginalPosition == false)
            {
                _originalPosition = this.transform.position;
                gotOriginalPosition = true;
            }
            Attack();
        }
    }

    private void CreateIncomingPath()
    {
        //Vector3 lastPoint = _waypoints[_waypoints.Count - 1];
        _waypoints.Clear();

        Vector3 topSide = Camera.main.ViewportToWorldPoint(new Vector3(0, 1, 0));
        _waypoints.Add(new Vector3(-1.289417f, 0, topSide.z));
        _waypoints.Add(_originalPosition);
        _isOnPath = true;
    }

    /// <summary>
    /// Attack following player and then after getting close just go 
    /// towards the bottom of screen.
    /// </summary>
    public void Attack()
    {
        transform.LookAt(player);
        //move towards the center of the world (or where ever you like)
        //Vector3 targetPosition = new Vector3(0, 0, 0);
        Vector3 targetPosition = player.transform.position;
        Vector3 currentPosition = this.transform.position;
        
        //first, check to see if we're close enough to the target
        if (Vector3.Distance(currentPosition, targetPosition) > 24.0f && outOfPlayerRange == false)
        {
            Vector3 directionOfTravel = targetPosition - currentPosition;
            //now normalize the direction, since we only want the direction information
            directionOfTravel.Normalize();
            //scale the movement on each axis by the directionOfTravel vector components

            this.transform.Translate(
                (directionOfTravel.x * swoopSpeed * Time.deltaTime),
                (directionOfTravel.y * swoopSpeed * Time.deltaTime),
                (directionOfTravel.z * swoopSpeed * Time.deltaTime),
                Space.World);
            //outOfPlayerRange = true;
        }
        else
        {
            outOfPlayerRange = true;
            if (outOfPlayerRange)
            {
                //Debug.Log("Not close anymore to player".Colored(Colors.red));
                targetPosition = enemyProjWall.transform.position;
                Vector3 directionAfterPlayer = targetPosition - currentPosition;
                directionAfterPlayer.Normalize();
                this.transform.Translate(
                    (directionAfterPlayer.x * swoopSpeed * Time.deltaTime),
                    (directionAfterPlayer.y * swoopSpeed * Time.deltaTime),
                    (directionAfterPlayer.z * swoopSpeed * Time.deltaTime),
                    Space.World);
                transform.LookAt(enemyProjWall);
            }
            Debug.Log(gameObject.transform.position.z.ToString().Bold());
            if (gameObject.transform.position.z < -70f)
            {
                Debug.Log("Enemy made it to wall".Bold());
                CreateIncomingPath();
                
            }
        }

        if (_isOnPath)
        {
            Debug.Log("Is on path now".Bold());
            iTween.PutOnPath(gameObject, _waypoints.ToArray(), _pathPercentage);
            _pathPercentage += Time.deltaTime * 10f / 10;
            //Debug.Log("path Percent: " + _pathPercentage);
            if (_pathPercentage > 1)
            {
                _isOnPath = false;
                //_finishedPath = false;
                _pathPercentage = 0;
                outOfPlayerRange = false;
                gotOriginalPosition = false;
                AttackPlayer = false;
                //transform.rotation = _originalRotation;
            }
        }
    }
}