using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class L1BossAI : DealDamageCont_E, IFTakeDamage_E
{
    public L1BossAtkBase _currentAtk;
    //public L1BossAtk1 _atk1 = new();
    public L1BossAtk123 _atk123 = new();
    //public L1BossAtk3 _atk3 = new();
    public L1BossAtk456 _atk456 = new();

    #region Preset
    [Header("Preset")]
    [Header("Level 1 Boss Preset")]
    [Tooltip("Level 1 Boss Preset")]
    [SerializeField] private GameObject _ninaGO;
    [SerializeField] private Transform _frontFP;
    [SerializeField] private Transform _bottomFP;
    [SerializeField] private Transform _bottomLeftFP;
    [SerializeField] private Transform _topLeftFP;
    [SerializeField] private Transform _topRightFP;
    [SerializeField] private Transform _leftPos;
    [SerializeField] private Transform _rightPos;
    [SerializeField] private Animator _l1BossAnimtr;
    [SerializeField] private GameObject _deathLight;

    //[Space(5f)]
    [Header("Attack 1 Preset")]
    [Tooltip("Attack 1 Preset")]
    [SerializeField] private GameObject _atk1LaserPrefab;
    //[SerializeField] private Sprite _shootingSprite;

    //[Space(5f)]
    [Header("Attack 2 Preset")]
    [Tooltip("Attack 2 Preset")]
    [SerializeField] private GameObject _grenadePrefab;

    [Header("Attack 3 Preset")]
    [Tooltip("Attack 3 Preset")]
    [SerializeField] private GameObject _energyBallPrefab;
    [SerializeField] private Transform _atk3RightPos01;
    [SerializeField] private Transform _atk3RightPos02;
    [SerializeField] private Transform _atk3LeftPos01;
    [SerializeField] private Transform _atk3LeftPos02;

    [Header("Attack 4 Preset")]
    [Tooltip("Attack 4 Preset")]
    [SerializeField] private GameObject _dronePrefab;
    [SerializeField] private GameObject _emptyGOPrefab;

    [Header("Attack 5 Preset")]
    [Tooltip("Attack 5 Preset")]
    [SerializeField] private GameObject _atk5LaserPrefab;
    [SerializeField] private GameObject _atk5BeamPrefab;
    [SerializeField] private Transform _atk5LeftPos;
    [SerializeField] private Transform _atk5RightPos;
    [SerializeField] private Transform[] _atk5CurvePoints;


    [Header("Attack 6 Preset")]
    [Tooltip("Attack 6 Preset")]
    [SerializeField] private Transform _atk6TopPos;
    [SerializeField] private Transform _atk6GroundPos;
    [SerializeField] private GameObject _atk6WarnSignPrefab;
    [SerializeField] private GameObject _atk6LaserPrefab;
    [SerializeField] private GameObject _atk6GrenadePrefab;

    #endregion

    #region Config
    [Header("Config")]
    [Header("L1Boss Statistics Config")]
    [Tooltip("L1Boss Statistics Config")]
    public float _maxL1BossHP;
    public float _currentL1BossHP;
    public float _l1BossHPRegenRate;
    [SerializeField] private int _atk6Threshold = 3;

    [Header("Level 1 Boss Config")]
    [Tooltip("Level 1 Boss Config")]
    [SerializeField] private bool _isResetAnim;
    [SerializeField] private bool _startAtk;
    [SerializeField] private float _l1BossMoveSpd;
    [SerializeField] private float _l1BossRotateSpd;
    [SerializeField] private float _l1BossMinDistance;
    [Space(5f)]
    [SerializeField] private float _atk1CD = 2f;
    [SerializeField] private float _atk2CD = 2f;
    [SerializeField] private float _atk3CD = 2f;
    [SerializeField] private float _atk4CD = 2f;
    [SerializeField] private float _atk5CD = 2f;
    [SerializeField] private float _atk6CD = 2f;

    //[Space(5f)]
    [Header("Attack 1 Config")]
    [Tooltip("Attack 1 Config")]
    [SerializeField] private float _atk1LaserScaleCD;
    [SerializeField] private float _atk1LaserScaleSpd;
    [SerializeField] private int _loopNum;

    [Header("Attack 2 Config")]
    [Tooltip("Attack 2 Config")]
    [SerializeField] private float _grenadeMoveSpd = 1f;
    [SerializeField] private float _grenadeRotateSpd;

    [Header("Attack 3 Config")]
    [Tooltip("Attack 3 Config")]
    [SerializeField] private float _energyBallMoveSpd = 1f;
    [SerializeField] private float _energyBallRotateSpd;
    //[SerializeField] private float _minDistance;


    [Header("Attack 4 Config")]
    [Tooltip("Attack 4 Config")]
    [SerializeField] private float _droneMoveSpd = 1f;
    [SerializeField] private float _droneRotateSpd;
    [SerializeField] private float[] _droneSpawnXMin;
    [SerializeField] private float[] _droneSpawnXMax;
    [SerializeField] private float[] _droneSpawnYMin;
    [SerializeField] private float[] _droneSpawnYMax;
    [SerializeField] private int _maxDroneNum = 3;

    [Header("Attack 5 Config")]
    [Tooltip("Attack 5 Config")]
    //[SerializeField] private float _curveWidth = 10.0f; // Width of the U-shape
    //[SerializeField] private float _curveHeight = 5.0f; // Height of the U-shape
    [SerializeField] private float _interpolateSpd;

    [Header("Attack 6 Config")]
    [Tooltip("Attack 6 Config")]
    [SerializeField] private float _l1BossFollowDuration = 4f;
    [SerializeField] private float _atk6LaserDelay = 1f;
    [SerializeField] private float _atk6LaserDuration = 1f;
    [SerializeField] private float _atk6GSpawnDelay = 1f;
    [SerializeField] private int _atk6GTotalNum = 4;

    //[SerializeField] private Quaternion _atk5LaserRotation;
    //[Space(30f)]

    #endregion


    #region Debug
    [Header("Debug")]
    [Tooltip("L1Boss Debug")]
    [SerializeField] private bool _isSpawningL1Boss;
    [SerializeField] private Rigidbody2D _l1BossRb2D;
    [SerializeField] private int _currentAtkNum;
    [SerializeField] private bool _isAttacking;
    [SerializeField] private int _prevAtkNum;
    [SerializeField] private int _atkLaunchedNum;
    [SerializeField] private bool _halfHPAtk6Launched;
    [SerializeField] private float _l1BossCurrentDist;
    [SerializeField] private List<int> _atkArrangement = new() { 1, 2, 3, 4, 5 };

    public static bool _isDoneAtking;

    [Header("Attack 1 Debug")]
    [Tooltip("Attack 1 Debug")]
    [SerializeField] private GameObject _atk1LaserGO;
    [SerializeField] private Coroutine _atkCDCoroutine;

    [Header("Attack 2 Debug")]
    [Tooltip("Attack 2 Debug")]
    [SerializeField] private GameObject _grenadeGO;
    [SerializeField] private Rigidbody2D _grenadeRb2D;

    [Header("Attack 3 Debug")]
    [Tooltip("Attack 3 Debug")]
    [SerializeField] private GameObject _energyBallGO;
    [SerializeField] private Rigidbody2D _energyBallRb2D;
    [SerializeField] private int _atk3Movement;
    [SerializeField] private bool _isFireAtk3EB;


    [Header("Attack 4 Debug")]
    [Tooltip("Attack 4 Debug")]
    [SerializeField] public List<Vector3> _droneSpawnPos;
    [SerializeField] public List<GameObject> _droneGOs;
    [SerializeField] private int _currentDroneNum;
    [SerializeField] private int _droneNumToSpawn;
    [SerializeField] private bool _isSpawnedDrone;
    public int _nextDroneSpawnID;

    [Header("Attack 5 Debug")]
    [Tooltip("Attack 5 Debug")]
    [SerializeField] private bool _isStartAtk5;
    [SerializeField] private bool _isFireAtk5Laser;
    [SerializeField] private bool _isFireAtk5Beam;
    [SerializeField] private int _atk5Movement;
    [SerializeField] private GameObject _atk5LaserGO;
    [SerializeField] private GameObject _atk5BeamGO;
    [SerializeField] private float _interpolateAmount;
    [SerializeField] private int _curvePointNum;


    [Header("Attack 6 Debug")]
    [Tooltip("Attack 6 Debug")]
    [SerializeField] private bool _isStartAtk6;
    [SerializeField] private bool _canFireAtk6Laser;
    [SerializeField] private bool _isFiringAtk6Laser;
    [SerializeField] private bool _isOnAtk6LaserCoroutine;
    [SerializeField] private int _atk6LaserCoroutineCount;
    [SerializeField] private int _atk6DoneLaserCoroutineCount;
    [SerializeField] private bool _isFireAtk6Grenades;
    [SerializeField] private int _grenadeCount;
    [SerializeField] private int _atk6Movement;
    [SerializeField] public int _atk6LaserCount;
    [SerializeField] private float _l1BossFollowTimeCount;
    [SerializeField] private GameObject _atk6LaserGO;
    [SerializeField] public List<GameObject> _atk6GrenadeGOs;
    private Coroutine _atk6GrenadeCoroutine;

    [Header("Static")]
    [Tooltip("L1Boss Static")]
    public static bool _isFlippedL1Boss;
    public static bool _l1BossIsDead;

    [Tooltip("Attack 6 Static")]
    public static bool _atk6GrenadeCanChase;

    #endregion

    // Start is called before the first frame update
    void Start()
    {
        //_currentL1BossHP = _maxL1BossHP;
        _currentAtk = _atk123;
        _currentAtk.EnterForm(this);
        //_l1BossAnimtr = GetComponent<Animator>();
        _l1BossRb2D = GetComponent<Rigidbody2D>();
        _ninaGO = GameObject.FindGameObjectWithTag("Player");
    }
    private void OnEnable()
    {
        _isSpawningL1Boss = true;
        _ninaGO = GameObject.FindGameObjectWithTag("Player");
    }
    // Update is called once per frame
    void Update()
    {
        SpawnBoss();

        _currentAtk.UpdateForm(this);

        //ResetBoolParameters();

        _currentDroneNum = _droneGOs.Count;

        //Debug.Log("_isFlippedL1Boss = " + _isFlippedL1Boss);

        //if(transform.localScale.x == -1)
        //{
        //    _isFlippedL1Boss = true;
        //}
        //else
        //{
        //    _isFlippedL1Boss = false;
        //}

        if (_startAtk)
        {
            Check_SwitchAttack(_isAttacking);
        }

        if (_currentL1BossHP <= 0)
        {
            //_l1BossIsDead = true;
            Menus.PauseGame(true, 0f);
            Menus._otherMenuOn = true;
            _deathLight.SetActive(true);
            ////Destroy(gameObject);
        }
    }
    private void Check_SwitchAttack(bool isAttacking)
    {
        if (!isAttacking)
        {
            switch (_currentAtkNum)
            {
                default:
                    SwitchAttack(_atk123);

                    _l1BossAnimtr.SetBool("isShooting", true);
                    _l1BossAnimtr.SetBool("isIdle", false);
                    //gameObject.GetComponent<SpriteRenderer>().sprite = _shootingSprite;
                    
                    //_atk1LaserGO = _atk123.SpawnProjectile(_atk1LaserPrefab, _frontFP.position, _frontFP.rotation);
                    _atk1LaserGO = _atk456.SpawnProjectileAsChild(_atk1LaserPrefab, _frontFP.position, _frontFP.rotation, transform);
                    
                    if (AudioManager.amInstance != null)
                    {
                        AudioManager.amInstance.PlaySF("Xuanwu's laser beam");
                    }
                    //StartCoroutine(StartLaserScale(_laserScaleCD, _loopNum));
                    _isDoneAtking = true;

                    AtkCDCheck(_atk1CD);

                    break;
                
                case 2:
                    SwitchAttack(_atk123);

                    _grenadeGO = _atk123.SpawnProjectile(_grenadePrefab, _frontFP.position, _frontFP.rotation);
                    _grenadeRb2D = _grenadeGO.GetComponent<Rigidbody2D>();
                    Vector3 grenadeDir = _atk123.HomingProjectileDir(_ninaGO.transform.position, _grenadeGO.transform.position);
                    _grenadeRb2D.velocity = new Vector2(grenadeDir.x * _grenadeMoveSpd, grenadeDir.y * _grenadeMoveSpd);
                    _grenadeRb2D.angularVelocity = _atk123.AimTarget(_grenadeGO.transform.right, _grenadeRotateSpd, grenadeDir);
 
                    _isDoneAtking = true;

                    AtkCDCheck(_atk2CD);
                    break;
                
                case 3:
                    SwitchAttack(_atk123);

                    //_l1BossAnimtr.SetBool("atk3", true);
                        Atk3Movement();

                        if (_isFireAtk3EB)
                        {
                            _energyBallGO = _atk123.SpawnProjectile(_energyBallPrefab, _frontFP.position, _frontFP.rotation);
                            _energyBallRb2D = _energyBallGO.GetComponent<Rigidbody2D>();
                            Vector3 energyBallDir = _atk123.HomingProjectileDir(_ninaGO.transform.position, _energyBallGO.transform.position);
                            _energyBallRb2D.velocity = new Vector2(energyBallDir.x * _energyBallMoveSpd, energyBallDir.y * _energyBallMoveSpd);
                            _energyBallRb2D.angularVelocity = _atk123.AimTarget(_energyBallGO.transform.right, _energyBallRotateSpd, energyBallDir);
                            _isFireAtk3EB = false;
                        }

                    break;
                
                case 4:
                    SwitchAttack(_atk456);

                    if (_currentDroneNum < _maxDroneNum)
                    {
                        _droneNumToSpawn = _maxDroneNum - _currentDroneNum;
                        //Debug.Log("_droneNumToSpawn: " + _droneNumToSpawn);
                        for (int i = _nextDroneSpawnID; i < _droneNumToSpawn; i++)
                        {
                            _droneSpawnPos[i] = _atk456.RandomPos(_droneSpawnXMin[i], _droneSpawnXMax[i], _droneSpawnYMin[i], _droneSpawnYMax[i]);
                            GameObject dronePosGO = Instantiate(_emptyGOPrefab, _droneSpawnPos[i], Quaternion.identity);
                            GameObject tempGO = _atk456.SpawnProjectile(_dronePrefab, transform.position, _dronePrefab.transform.rotation);
                            tempGO.GetComponent<L1Boss_Atk4_Drone>()._targetPos = _droneSpawnPos[i];
                            _droneGOs.Add(tempGO);
                            //_droneGOs[i] = tempGO;

                            Rigidbody2D droneRb2D = tempGO.GetComponent<Rigidbody2D>();
                            Vector3 droneDir = _atk456.HomingProjectileDir(dronePosGO.transform.position, tempGO.transform.position);
                            droneRb2D.velocity = new Vector2(droneDir.x * _droneMoveSpd, droneDir.y * _droneMoveSpd);
                            droneRb2D.angularVelocity = _atk456.AimTarget(tempGO.transform.right, _droneRotateSpd, droneDir);
                        }

                        //_isDoneAtking = true;
                        //AtkCDCheck(_atk4CD);

                    }

                    else
                    {
                        _isDoneAtking = true;
                        AtkCDCheck(_atk4CD);
                    }

                    break;
                
                case 5:
                    SwitchAttack(_atk456);

                    //Debug.Log(_atk5Movement);
                    //if (!_isStartAtk5)
                    //{
                    //    _l1BossAnimtr.SetInteger("atk5", 1);
                    //    _isStartAtk5 = true;
                    //}
                        Atk5Movement();

                        if (_isFireAtk5Laser)
                        {
                            _atk5LaserGO = _atk456.SpawnProjectileAsChild(_atk5LaserPrefab, _bottomLeftFP.position, _bottomLeftFP.rotation, transform);
                        
                            if (AudioManager.amInstance != null)
                            {
                                AudioManager.amInstance.PlaySF("Xuanwu's laser beam");
                            }

                        _isFireAtk5Laser = false;
                        }

                        if (_isFireAtk5Beam)
                        {
                            _atk5BeamGO = _atk456.SpawnProjectileAsChild(_atk5BeamPrefab, _frontFP.position, _frontFP.rotation, transform);
                            
                        if (AudioManager.amInstance != null)
                            {
                                AudioManager.amInstance.PlaySF("Xuanwu's laser beam 2");
                            }

                        _isFireAtk5Beam = false;
                        }

                    break;
                
                case 6:
                    SwitchAttack(_atk456);

                        Atk6Movement();

                        if (_canFireAtk6Laser)
                        {
                            StartCoroutine(Atk6ShootLaser(_atk6LaserDelay, _atk6LaserDuration));
                            //_isFireAtk6Laser = false;
                        }

                        if (_isFireAtk6Grenades)
                        {
                            StartCoroutine(Atk6ShootGrenade(_atk6GTotalNum, _atk6GSpawnDelay));
                        }


                        if (!_halfHPAtk6Launched)
                        {
                            _halfHPAtk6Launched = true;
                        }

                    break;
            }
        }
    }

    private void SpawnBoss()
    {
        if (_isSpawningL1Boss)
        {
            _currentL1BossHP += _maxL1BossHP / 2 * Time.deltaTime;

            if (_currentL1BossHP >= _maxL1BossHP)
            {
                _currentL1BossHP = _maxL1BossHP;
                _currentAtkNum = RandomAttack();
                _startAtk = true;
                _isSpawningL1Boss = false;
            }
        }
    }
    private void SwitchAttack(L1BossAtkBase atk)
    {
        _currentAtk = atk;
        atk.EnterForm(this);
    }
    private int RandomAttack()
    {
        int randomAtkNum = 0;
        int randomIndex;
        _prevAtkNum = _currentAtkNum;
        
        if (_currentL1BossHP > _maxL1BossHP / 2)
        {
            if (_atkArrangement.Count > 0)
            {
                // Generate a random index to pick a number from the list
                randomIndex = Random.Range(0, _atkArrangement.Count);

                // Get the random number from the list
                randomAtkNum = _atkArrangement[randomIndex];

                // Remove the picked number from the list
                _atkArrangement.RemoveAt(randomIndex);

                ////Debug.Log("Picked number: " + randomAtkNum);
                
                return randomAtkNum;
            }
            else
            {
                //Debug.Log("Number list is empty.");
                _atkArrangement = new() { 1, 2, 3, 4, 5 };

                //Debug.Log("_atkArrangement = " + _atkArrangement);

                // Generate a random index to pick a number from the list
                randomIndex = Random.Range(0, _atkArrangement.Count);

                // Get the random number from the list
                randomAtkNum = _atkArrangement[randomIndex];

                // Remove the picked number from the list
                _atkArrangement.RemoveAt(randomIndex);

                ////Debug.Log("Picked number: " + randomAtkNum);

                return randomAtkNum;
            }
        }
        else if (_currentL1BossHP <= _maxL1BossHP / 2)
        {
            if (!_halfHPAtk6Launched)
            {
                randomAtkNum = 6;
                _atkArrangement = new() { 1, 2, 3, 4, 5 };

                return randomAtkNum;
            }
            else if (_halfHPAtk6Launched && _atkArrangement.Count > 0)
            {
                // Generate a random index to pick a number from the list
                randomIndex = Random.Range(0, _atkArrangement.Count);

                // Get the random number from the list
                randomAtkNum = _atkArrangement[randomIndex];

                // Remove the picked number from the list
                _atkArrangement.RemoveAt(randomIndex);

                ////Debug.Log("Picked number after Atk 6: " + randomAtkNum);

                return randomAtkNum;
            }
            else
            {
                //Debug.Log("Number list is empty after Atk 6.");
                _atkArrangement = new() { 1, 2, 3, 4, 5, 6 };

                // Generate a random index to pick a number from the list
                randomIndex = Random.Range(0, _atkArrangement.Count);

                // Get the random number from the list
                randomAtkNum = _atkArrangement[randomIndex];

                // Remove the picked number from the list
                _atkArrangement.RemoveAt(randomIndex);

                ////Debug.Log("Picked number: " + randomAtkNum);

                return randomAtkNum;
            }
        }

        return randomAtkNum;
    }
    private IEnumerator StartAtkCD(float cd)
    {
        ////Debug.Log("Waiting for CD");
        //_isStartAtkCD = true;
        yield return new WaitForSeconds(cd);
        ////Debug.Log("CD done");

        _currentAtkNum = RandomAttack();
        //Debug.Log("_currentAtkNum = " + _currentAtkNum);
        
        _atk3Movement = 0;
        _atk5Movement = 0;
        _atk6Movement = 0;

        _isAttacking = false;
    }

    private void StopAtkCDCoroutine()
    {
        if(_atkCDCoroutine != null)
        {
            StopCoroutine(_atkCDCoroutine);
            _atkCDCoroutine = null;
        }
    }

    private void StopAtk6GrenadeCoroutine()
    {
        if (_atk6GrenadeCoroutine != null)
        {
            StopCoroutine(_atk6GrenadeCoroutine);
            _atk6GrenadeCoroutine = null;
        }
    }

    private void AtkCDCheck(float atkCD)
    {
        if (_isDoneAtking)
        {
            //_atkCDCoroutine = StartCoroutine(StartAtkCD(_atkCD));
            StartCoroutine(StartAtkCD(atkCD)); 
            _isAttacking = true;
            //Debug.Log("_isAttacking = " + _isAttacking);
            _isDoneAtking = false;
            //Debug.Log("_isDoneAtking = " + _isDoneAtking);

            ////_currentAtkNum = RandomAttack();
        }
    }
    public void FlipBoss()
    {
        Vector3 newScale = new(-transform.localScale.x, transform.localScale.y, transform.localScale.z);
        transform.localScale = newScale;
    }
    public void Atk3Movement()
    {
        if (_atk3Movement == 0)
        {
            if (!_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_atk3RightPos01.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    Vector3 playerDir01 = _atk123.HomingProjectileDir(_atk3RightPos01.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir01.x * _l1BossMoveSpd, playerDir01.y * _l1BossMoveSpd);
                    //Debug.Log("Atk3 Flying" + _l1BossRb2D.velocity);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossRb2D.velocity = Vector2.zero;
                    _isFireAtk3EB = true;
                    //Debug.Log("Atk3 Stop Flying");
                    _atk3Movement = 1;
                }
            }

            else if (_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_atk3LeftPos01.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    Vector3 playerDir01 = _atk123.HomingProjectileDir(_atk3LeftPos01.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir01.x * _l1BossMoveSpd, playerDir01.y * _l1BossMoveSpd);
                    //Debug.Log("Atk3 Flying" + _l1BossRb2D.velocity);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossRb2D.velocity = Vector2.zero;
                    _isFireAtk3EB = true;
                    //Debug.Log("Atk3 Stop Flying");
                    _atk3Movement = 1;
                }
            }
        }
        else if (_atk3Movement == 1)
        {
            if (!_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_atk3RightPos02.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    Vector3 playerDir02 = _atk123.HomingProjectileDir(_atk3RightPos02.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir02.x * _l1BossMoveSpd, playerDir02.y * _l1BossMoveSpd * 4);
                    //Debug.Log("Atk3 Flying" + _l1BossRb2D.velocity);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossRb2D.velocity = Vector2.zero;
                    //Debug.Log("Atk3 Stop Flying");
                    _atk3Movement = 2;
                }
            }
            else if (_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_atk3LeftPos02.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    Vector3 playerDir02 = _atk123.HomingProjectileDir(_atk3LeftPos02.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir02.x * _l1BossMoveSpd, playerDir02.y * _l1BossMoveSpd * 4);
                    //Debug.Log("Atk3 Flying" + _l1BossRb2D.velocity);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossRb2D.velocity = Vector2.zero;
                    //Debug.Log("Atk3 Stop Flying");
                    _atk3Movement = 2;
                }
            }
        }
        else if (_atk3Movement == 2)
        {
            if (!_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_atk3RightPos01.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    Vector3 playerDir01 = _atk123.HomingProjectileDir(_atk3RightPos01.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir01.x * _l1BossMoveSpd, playerDir01.y * _l1BossMoveSpd);
                    //Debug.Log("Atk3 Flying" + _l1BossRb2D.velocity);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossRb2D.velocity = Vector2.zero;
                    //Debug.Log("Atk3 Stop Flying");
                    _atk3Movement = 3;
                }
            }
            else if (_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_atk3LeftPos01.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    Vector3 playerDir01 = _atk123.HomingProjectileDir(_atk3LeftPos01.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir01.x * _l1BossMoveSpd, playerDir01.y * _l1BossMoveSpd);
                    //Debug.Log("Atk3 Flying" + _l1BossRb2D.velocity);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossRb2D.velocity = Vector2.zero;
                    //Debug.Log("Atk3 Stop Flying");
                    _atk3Movement = 3;
                }
            }
        }
        else if (_atk3Movement == 3)
        {
            if (!_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_rightPos.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    Vector3 playerDir01 = _atk123.HomingProjectileDir(_rightPos.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir01.x * _l1BossMoveSpd, playerDir01.y * _l1BossMoveSpd);
                    //Debug.Log("Atk3 Flying" + _l1BossRb2D.velocity);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossRb2D.velocity = Vector2.zero;
                    //Debug.Log("Atk3 Stop Flying");
                    _atk3Movement = 4;
                    _isDoneAtking = true;
                    AtkCDCheck(_atk3CD);
                }
            }
            else if (_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_leftPos.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    Vector3 playerDir01 = _atk123.HomingProjectileDir(_leftPos.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir01.x * _l1BossMoveSpd, playerDir01.y * _l1BossMoveSpd);
                    //Debug.Log("Atk3 Flying" + _l1BossRb2D.velocity);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossRb2D.velocity = Vector2.zero;
                    //Debug.Log("Atk3 Stop Flying");
                    _atk3Movement = 4;
                    _isDoneAtking = true;
                    AtkCDCheck(_atk3CD);
                }
            }
        }
    }


    public void Atk5Movement()
    {
        if (_atk5Movement == 0)
        {
            if (!_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_atk5RightPos.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    _l1BossAnimtr.SetBool("isIdle", false);
                    _l1BossAnimtr.SetBool("isJumpingAtk5", true);
                    Vector3 playerDir01 = _atk123.HomingProjectileDir(_atk5RightPos.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir01.x * _l1BossMoveSpd, playerDir01.y * _l1BossMoveSpd);
                    //Debug.Log("Atk5 Flying" + _l1BossRb2D.velocity);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossRb2D.velocity = Vector2.zero;
                    _isFireAtk5Laser = true;
                    //Debug.Log("Atk5 Stop Flying");
                    _atk5Movement = 1;
                }
            }
            else if (_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_atk5LeftPos.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    _l1BossAnimtr.SetBool("isIdle", false);
                    _l1BossAnimtr.SetBool("isJumpingAtk5", true);
                    Vector3 playerDir01 = _atk123.HomingProjectileDir(_atk5LeftPos.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir01.x * _l1BossMoveSpd, playerDir01.y * _l1BossMoveSpd);
                    //Debug.Log("Atk5 Flying" + _l1BossRb2D.velocity);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossRb2D.velocity = Vector2.zero;
                    _isFireAtk5Laser = true;
                    //Debug.Log("Atk5 Stop Flying");
                    _atk5Movement = 1;
                }
            }
        }
        else if (_atk5Movement == 1)
        {
            if (!_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_atk5LeftPos.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    _l1BossAnimtr.SetBool("isJumpingAtk5", false);
                    _l1BossAnimtr.SetBool("isGShooting", true);
                    //StartCoroutine(CurveMovement());
                    CurveMovement();
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    //_l1BossRb2D.velocity = Vector2.zero;
                    _atk5LaserGO.SetActive(false);
                    //Debug.Log("Atk5 Stop Flying");
                    _isFireAtk5Beam = true;
                    //Debug.Log(_isFireAtk5Beam);
                    FlipBoss();
                    _interpolateAmount = 0;
                    
                    _atk5Movement = 2;
                }
            }
            else if (_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_atk5RightPos.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    _l1BossAnimtr.SetBool("isJumpingAtk5", false);
                    _l1BossAnimtr.SetBool("isGShooting", true);
                    //StartCoroutine(CurveMovement());
                    CurveMovement();
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    //_l1BossRb2D.velocity = Vector2.zero;
                    _atk5LaserGO.SetActive(false);
                    //Debug.Log("Atk5 Stop Flying");
                    _isFireAtk5Beam = true;
                    //Debug.Log(_isFireAtk5Beam);
                    FlipBoss();
                    _interpolateAmount = 0;
                    
                    _atk5Movement = 2;
                }
            }
        }
        else if (_atk5Movement == 2)
        {
            if (!_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_leftPos.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    _l1BossAnimtr.SetBool("isGShooting", false);
                    _l1BossAnimtr.SetBool("isLSiAiring", true);
                    Vector3 playerDir04 = _atk123.HomingProjectileDir(_leftPos.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir04.x * _l1BossMoveSpd, playerDir04.y * _l1BossMoveSpd);
                    //Debug.Log("Atk5 Dropping" + _l1BossRb2D.velocity);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossAnimtr.SetBool("isLSiAiring", false);
                    _l1BossAnimtr.SetBool("isIdle", true);
                    _l1BossRb2D.velocity = Vector2.zero;

                    //var beams = GameObject.FindGameObjectsWithTag("L1BossAtk5Beam");
                    //foreach (GameObject beamGO in beams)
                    //{
                    //    beamGO.SetActive(false);
                    //    Debug.Log("Deactivated");
                    //}

                    _atk5BeamGO.SetActive(false);
                    //Debug.Log("Atk5 Stop Dropping");
                    _atk5Movement = 3;
                    
                    _isFlippedL1Boss = !_isFlippedL1Boss;
                    _isDoneAtking = true;
                    AtkCDCheck(_atk5CD);
                }
            }
            else if (_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_rightPos.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    _l1BossAnimtr.SetBool("isGShooting", false);
                    _l1BossAnimtr.SetBool("isLSiAiring", true);
                    Vector3 playerDir04 = _atk123.HomingProjectileDir(_rightPos.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir04.x * _l1BossMoveSpd, playerDir04.y * _l1BossMoveSpd);
                    //Debug.Log("Atk5 Dropping" + _l1BossRb2D.velocity);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossAnimtr.SetBool("isLSiAiring", false);
                    _l1BossAnimtr.SetBool("isIdle", true);
                    _l1BossRb2D.velocity = Vector2.zero;

                    //var beams = GameObject.FindGameObjectsWithTag("L1BossAtk5Beam");
                    //foreach (GameObject beamGO in beams)
                    //{
                    //    beamGO.SetActive(false);
                    //    Debug.Log("Deactivated");
                    //}

                    _atk5BeamGO.SetActive(false);
                    //Debug.Log("Atk5 Stop Dropping");

                    _atk5Movement = 3;
                    
                    _isFlippedL1Boss = !_isFlippedL1Boss;
                    _isDoneAtking = true;
                    AtkCDCheck(_atk5CD);
                }
            }
        }
    }
    public void Atk6Movement()
    {
        if (_atk6Movement == 0)
        {
            _l1BossCurrentDist = _atk123.CalculateDistance(_atk6TopPos.position, transform.position);

            if (_l1BossCurrentDist > _l1BossMinDistance)
            {
                _l1BossAnimtr.SetBool("isIdle", false);
                _l1BossAnimtr.SetBool("isJumpingAtk6", true);
                Vector3 playerDir01 = _atk123.HomingProjectileDir(_atk6TopPos.position, transform.position);
                _l1BossRb2D.velocity = new Vector2(playerDir01.x * _l1BossMoveSpd, playerDir01.y * _l1BossMoveSpd);
            }

            else if (_l1BossCurrentDist <= _l1BossMinDistance)
            {
                _l1BossRb2D.velocity = Vector2.zero;
                _isFireAtk6Grenades = true;
                _atk6Movement = 1;
            }
        }
        else if (_atk6Movement == 1)
        {
            //Debug.Log(_atk6LaserCount);

            if (_atk6LaserCount < 3)
            {
                if (_l1BossFollowTimeCount < _l1BossFollowDuration)
                {
                    Vector3 playerDir02 = _atk123.HomingProjectileDir(_ninaGO.transform.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir02.x * _l1BossMoveSpd, 0);

                    _l1BossAnimtr.SetBool("isJumpingAtk6", false);
                    _l1BossAnimtr.SetBool("isLBIdle", true);
                    _l1BossFollowTimeCount += Time.deltaTime;
                }
                else if (_l1BossFollowTimeCount >= _l1BossFollowDuration && !_isFiringAtk6Laser)
                {
                    _l1BossAnimtr.SetBool("isLBIdle", false);
                    _l1BossAnimtr.SetBool("isLBABShooting", true);

                    _canFireAtk6Laser = true;
                }

                if (_atk6GrenadeCanChase && _atk6GrenadeGOs.Count == 0)
                {
                    _atk6GrenadeCanChase = false;
                    _isFireAtk6Grenades = true;
                }
            }

            else
            {
                StopAtk6GrenadeCoroutine();
                _atk6GrenadeCanChase = true;

                _l1BossCurrentDist = _atk123.CalculateDistance(_atk6TopPos.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    Vector3 playerDir01 = _atk123.HomingProjectileDir(_atk6TopPos.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir01.x * _l1BossMoveSpd, playerDir01.y * _l1BossMoveSpd);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossRb2D.velocity = Vector2.zero;
                    _atk6Movement = 2;
                }
            }
        }
        else if (_atk6Movement == 2)
        {
            if (!_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_leftPos.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    _l1BossAnimtr.SetBool("isLBIdle", false);
                    _l1BossAnimtr.SetBool("isJumpingAtk6", true);

                    Vector3 playerDir01 = _atk123.HomingProjectileDir(_leftPos.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir01.x * _l1BossMoveSpd, playerDir01.y * _l1BossMoveSpd);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossRb2D.velocity = Vector2.zero;
                    _atk6Movement = 3;
                    
                    FlipBoss();
                    _isFlippedL1Boss = !_isFlippedL1Boss;
                    _isDoneAtking = true;
                    AtkCDCheck(_atk6CD);
                }
            }
            else if (_isFlippedL1Boss)
            {
                _l1BossCurrentDist = _atk123.CalculateDistance(_rightPos.position, transform.position);

                if (_l1BossCurrentDist > _l1BossMinDistance)
                {
                    _l1BossAnimtr.SetBool("isLBIdle", false);
                    _l1BossAnimtr.SetBool("isJumpingAtk6", true);

                    Vector3 playerDir01 = _atk123.HomingProjectileDir(_rightPos.position, transform.position);
                    _l1BossRb2D.velocity = new Vector2(playerDir01.x * _l1BossMoveSpd, playerDir01.y * _l1BossMoveSpd);
                }

                else if (_l1BossCurrentDist <= _l1BossMinDistance)
                {
                    _l1BossRb2D.velocity = Vector2.zero;
                    _atk6Movement = 3;

                    FlipBoss();
                    _isFlippedL1Boss = !_isFlippedL1Boss;
                    _isDoneAtking = true;
                    AtkCDCheck(_atk6CD);
                }
            }
        }
    }

    private void CurveMovement()
    {
        if (!_isFlippedL1Boss)
        {
            //_interpolateAmount = (_interpolateAmount + _interpolateSpd * Time.deltaTime) % 1f;
            _interpolateAmount = _interpolateAmount + _interpolateSpd * Time.deltaTime;
            Vector3 lerpA = Vector3.Lerp(_atk5CurvePoints[0].position, _atk5CurvePoints[1].position, _interpolateAmount);
            Vector3 lerpB = Vector3.Lerp(_atk5CurvePoints[1].position, _atk5CurvePoints[2].position, _interpolateAmount);
            Vector3 lerpC = Vector3.Lerp(_atk5CurvePoints[2].position, _atk5CurvePoints[3].position, _interpolateAmount);
            Vector3 lerpAB = Vector3.Lerp(lerpA, lerpB, _interpolateAmount);
            Vector3 lerpBC = Vector3.Lerp(lerpB, lerpC, _interpolateAmount);
            transform.position = Vector3.Lerp(lerpAB, lerpBC, _interpolateAmount);
            //Debug.Log("CurveMovement" + transform.position);
        }
        else
        {
            //_interpolateAmount = (_interpolateAmount + _interpolateSpd * Time.deltaTime) % 1f;
            _interpolateAmount = _interpolateAmount + _interpolateSpd * Time.deltaTime;
            Vector3 lerpA = Vector3.Lerp(_atk5CurvePoints[3].position, _atk5CurvePoints[2].position, _interpolateAmount);
            Vector3 lerpB = Vector3.Lerp(_atk5CurvePoints[2].position, _atk5CurvePoints[1].position, _interpolateAmount);
            Vector3 lerpC = Vector3.Lerp(_atk5CurvePoints[1].position, _atk5CurvePoints[0].position, _interpolateAmount);
            Vector3 lerpAB = Vector3.Lerp(lerpA, lerpB, _interpolateAmount);
            Vector3 lerpBC = Vector3.Lerp(lerpB, lerpC, _interpolateAmount);
            transform.position = Vector3.Lerp(lerpAB, lerpBC, _interpolateAmount);
            //Debug.Log("CurveMovement" + transform.position);
        }

        //Vector3 curvePos = Vector3.Lerp(lerpAB, lerpBC, _interpolateAmount);
        //Debug.Log(curvePos);
        //Vector3 playerDir02 = _atk456.HomingProjectileDir(curvePos, transform.position);
        //_l1BossRb2D.velocity = new Vector2(playerDir02.x * _l1BossMoveSpd, playerDir02.y * _l1BossMoveSpd);
        //_l1BossRb2D.velocity = new Vector2(curvePos.x * _l1BossMoveSpd, -curvePos.y * _l1BossMoveSpd);
    }

    //private IEnumerator CurveMovement()
    //{

    //    yield return new WaitForEndOfFrame();
    //}

    private IEnumerator Atk6ShootLaser(float laserDelay, float laserDuration)
    {
        _canFireAtk6Laser = false;
        _isFiringAtk6Laser = true;
        
        _l1BossRb2D.velocity = Vector2.zero;
        //Vector3 tempLaserPos = new(_ninaGO.transform.position.x, _bottomFP.position.y, _bottomFP.position.z);
        //GameObject atk6LaserGO = _atk456.SpawnProjectile(_atk6LaserPrefab, tempLaserPos, _bottomFP.rotation);
        GameObject atk6LaserGO = _atk456.SpawnProjectile(_atk6LaserPrefab, _bottomFP.position, _bottomFP.rotation);
        Vector3 tempWarnSignPos = new(atk6LaserGO.transform.position.x, _atk6GroundPos.position.y, atk6LaserGO.transform.position.z);
        atk6LaserGO.SetActive(false);
        GameObject tempWarnSignGO = _atk456.SpawnProjectile(_atk6WarnSignPrefab, tempWarnSignPos, _atk6GroundPos.rotation);
        
        if (AudioManager.amInstance != null)
        {
            AudioManager.amInstance.PlaySF("Xuanwu's drone's warning");
        }

        yield return new WaitForSeconds(laserDelay);
        tempWarnSignGO.SetActive(false);
        atk6LaserGO.SetActive(true);

        yield return new WaitForSeconds(laserDuration);
        //_atk6DoneLaserCoroutineCount = atk6LaserGO.GetComponent<L1Boss_Atk6_Laser>()._id;
        atk6LaserGO.SetActive(false);
        _isFiringAtk6Laser = false;
        _atk6LaserCount++;
        _l1BossFollowTimeCount = 0;
    }

    private IEnumerator Atk6ShootGrenade(int grenadeTotalNum, float spawnDelay)
    {
        _isFireAtk6Grenades = false;
        //_isOnAtk6LaserCoroutine = true;
        for (int i = 0; i < grenadeTotalNum; i++)
        {
            if (transform.position.x <= _atk6TopPos.position.x)
            {
                
                GameObject tempGO = Instantiate(_atk6GrenadePrefab, _topLeftFP.transform.position, _atk6GrenadePrefab.transform.rotation);
                tempGO.GetComponent<L1Boss_Atk6_Grenade>()._id = i;
                _atk6GrenadeGOs.Add(tempGO);
                //_grenadeCount++;
            }
            else
            {
                GameObject tempGO = Instantiate(_atk6GrenadePrefab, _topRightFP.transform.position, _atk6GrenadePrefab.transform.rotation);
                tempGO.GetComponent<L1Boss_Atk6_Grenade>()._id = i;
                _atk6GrenadeGOs.Add(tempGO);
                //_grenadeCount++;
            }

            yield return new WaitForSeconds(spawnDelay);
        }

        if (_atk6GrenadeGOs.Count == grenadeTotalNum)
        //if (_grenadeCount == grenadeTotalNum)
        {
            _atk6GrenadeCanChase = true;
        }
    }

    //public void EnableAtk6()
    //{
    //    _isFireAtk6 = true;
    //}

    private void ResetBoolParameters()
    {
        if (_isResetAnim)
        {
            if (_l1BossAnimtr == null)
            {
                Debug.LogWarning("Animator component not found.");
                return;
            }

            AnimatorControllerParameter[] parameters = _l1BossAnimtr.parameters;

            foreach (AnimatorControllerParameter parameter in parameters)
            {
                if (parameter.type == AnimatorControllerParameterType.Bool)
                {
                    _l1BossAnimtr.SetBool(parameter.name, false);
                }
            }

            foreach (AnimatorControllerParameter parameter in parameters)
            {
                if (parameter.type == AnimatorControllerParameterType.Int)
                {
                    _l1BossAnimtr.SetInteger(parameter.name, 0);
                }
            }
        }
    }

    //public void ResetAtk5()
    //{
    //    _isResetAnim = true;
    //    ResetBoolParameters();
    //    _atk5BeamGO.SetActive(false);
    //    Debug.Log("Anim Ints Reset");
    //}

    //private IEnumerator StartLaserScale(float cd, int loopNum)
    //{
    //    // Increase the size of the GameObject using the calculated scale increase.
    //    float scaleIncrease = _laserScaleSpd * Time.deltaTime;
    //    for (int i = 0; i < loopNum; i++)
    //    {
    //        _laserGO.transform.localScale += new Vector3(scaleIncrease, 0f, 0f);
    //        yield return new WaitForSeconds(cd);
    //    }

    //    _laserGO.SetActive(false);
    //}

    public void TakeDamage(float damage)
    {
        _currentL1BossHP -= damage;
    }


}