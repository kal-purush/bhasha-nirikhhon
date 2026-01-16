using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Experience_Ball : MonoBehaviour {
    [SerializeField] private int value;
     [SerializeField] private float minSpeed = 5f;
    [SerializeField] private float maxSpeed = 20f;
    [SerializeField] private float randomMovingTime = 3f;
    
    // [SerializeField, ReadOnly] private bool isSleep;
    [SerializeField, ReadOnly] private float time = 0f;
    [SerializeField, ReadOnly] private float speed;
    [SerializeField, ReadOnly] private Vector3 randomDirection;
    [SerializeField, ReadOnly] private Vector3 targetPosition;
    [SerializeField, ReadOnly] private GameObject player;
    [SerializeField, ReadOnly] private ExpManager expManager;
    [SerializeField, ReadOnly] private TrailRenderer trailRenderer;
    
    #region Unity Methodd

        private void Awake(){
            player = GameObject.FindGameObjectWithTag("Player");
            trailRenderer = GetComponent<TrailRenderer>();
        }

        public void Initialize(Transform playerTransform){
            expManager = ExpManager.instance;
            SetUpCollectable();
            gameObject.SetActive(true);
            speed = minSpeed;
        }

        private void Update(){

            if (time > randomMovingTime){
                MoveToPlayer();
            } else {
                MoveRandom();
            }
        }
    #endregion

    #region SetUp Collectable
        private void SetUpCollectable(){
            
            randomDirection = new Vector3(Random.Range(-10, 10), Random.Range(-10, 10), 0).normalized;
            targetPosition = transform.position + randomDirection;
            time = 0f;
            speed = minSpeed;
            trailRenderer.Clear();
        }
    #endregion

    #region Collectable Moving
        private void InsertSpeed(){
            if (speed <= maxSpeed){
                speed += 0.05f;
            }
        }

        private void MoveRandom(){
            Move(targetPosition);
            time += Time.deltaTime;
        }

        private void MoveToPlayer(){
            targetPosition = player.transform.position + new Vector3(0, 1.5f, 0);
            InsertSpeed();
            Move(targetPosition);
            CheckDistancePlayer(targetPosition);
        }

        private void Move(Vector3 targetPosition){
            transform.position = Vector3.MoveTowards(transform.position, targetPosition, speed * Time.deltaTime);
        }

        private void CheckDistancePlayer(Vector3 targetPosition){
            if  (Vector3.Distance(transform.position, targetPosition) < 0.1f){
                expManager.ChangeExp(value);
                gameObject.SetActive(false);
                
                SetUpCollectable();
            }
        }
    #endregion
}