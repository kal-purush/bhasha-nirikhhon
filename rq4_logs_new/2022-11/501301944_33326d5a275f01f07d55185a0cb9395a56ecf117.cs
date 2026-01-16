using System;
using UnityEngine;
using UnityEngine.AI;
using Unity.MLAgents.Actuators;
using Unity.MLAgents.Sensors;
using BodyPart = Unity.MLAgentsExamples.BodyPart;

public class AgentNavMeshRes : GenericAgent
{
    
    private NavMeshPath _path;
    private int _pathCornerIndex;
    private float _timeElapsed;
    private Vector3 _nextPathPoint;

    private GameObject targetBall;

    public bool resilience_training = true;
    private float last_bump = 0;

    public override void Initialize()
    {
        base.Initialize();
        _path = new NavMeshPath();
        _timeElapsed = 1f;
        _nextPathPoint = _topTransform.position;

        targetBall = GameObject.CreatePrimitive(PrimitiveType.Sphere);
        Destroy(targetBall.GetComponent<Collider>());
    }
    /// <summary>
    /// Add relevant information on each body part to observations.
    /// </summary>
    private void CollectObservationBodyPart(BodyPart bp, VectorSensor sensor)
    {
        //GROUND CHECK
        sensor.AddObservation(bp.groundContact.TouchingGround); // Is this bp touching the ground

        //Get velocities in the context of our orientation cube's space
        //Note: You can get these velocities in world space as well but it may not train as well.
        sensor.AddObservation(_orientationCube.transform.InverseTransformDirection(bp.rb.velocity));
        sensor.AddObservation(_orientationCube.transform.InverseTransformDirection(bp.rb.angularVelocity));

        //Get position relative to hips in the context of our orientation cube's space
        sensor.AddObservation(_orientationCube.transform.InverseTransformDirection(bp.rb.position - _topTransform.position));

        if (bp.rb.transform.GetComponent<Bone>().category != BoneCategory.Hand)
        {
            sensor.AddObservation(bp.rb.transform.localRotation);
            sensor.AddObservation(bp.currentStrength / _jdController.maxJointForceLimit);
        }
    }

    /// <summary>
    /// Loop over body parts to add them to observation.
    /// </summary>
    public override void CollectObservations(VectorSensor sensor)
    {
        var cubeForward = _orientationCube.transform.forward;

        //velocity we want to match
        var velGoal = cubeForward * MTargetWalkingSpeed;
        //ragdoll's avg vel
        var avgVel = GetAvgVelocityOfCreature();

        //current ragdoll velocity. normalized
        sensor.AddObservation(Vector3.Distance(velGoal, avgVel));
        //avg body vel relative to cube
        sensor.AddObservation(_orientationCube.transform.InverseTransformDirection(avgVel));
        //vel goal relative to cube
        sensor.AddObservation(_orientationCube.transform.InverseTransformDirection(velGoal));

        //rotation deltas
        sensor.AddObservation(Quaternion.FromToRotation(_topTransform.forward, cubeForward));

        //Position of target position relative to cube
        sensor.AddObservation(_orientationCube.transform.InverseTransformPoint(_nextPathPoint));

        foreach (var bodyPart in _jdController.bodyPartsList)
        {
            CollectObservationBodyPart(bodyPart, sensor);
            //rotation deltas for the head
            if (bodyPart.rb.transform.GetComponent<Bone>().category == BoneCategory.Head) sensor.AddObservation(Quaternion.FromToRotation(bodyPart.rb.transform.forward, cubeForward));
        }
    }

    public override void OnActionReceived(ActionBuffers actionBuffers)
    {
        var bpList = _jdController.bodyPartsList;
        var i = -1;

        var continuousActions = actionBuffers.ContinuousActions;
        // TODO Needs to be reworked for generalization
        foreach (var parts in bpList)
        {
            var xTarget = parts.joint.angularXMotion == ConfigurableJointMotion.Locked ? 0 : continuousActions[++i];
            var yTarget = parts.joint.angularYMotion == ConfigurableJointMotion.Locked ? 0 : continuousActions[++i];
            var zTarget = parts.joint.angularZMotion == ConfigurableJointMotion.Locked ? 0 : continuousActions[++i];
            parts.SetJointTargetRotation(xTarget, yTarget, zTarget);
            parts.SetJointStrength(continuousActions[++i]);
        }
    }

    public void FixedUpdate()
    {
        float deltaTime = Time.deltaTime;
        _timeElapsed += deltaTime;
        _nextPathPoint = GetNextPathPoint(_nextPathPoint);
        targetBall.transform.position = _nextPathPoint;

        //Update OrientationCube and DirectionIndicator
        _dirToWalk = _nextPathPoint - _topTransform.position;
        
        _orientationCube.UpdateOrientation(_topTransform.position, _nextPathPoint);

        var cubeForward = _orientationCube.transform.forward;

        // Set reward for this step according to mixture of the following elements.
        // a. Match target speed
        //This reward will approach 1 if it matches perfectly and approach zero as it deviates
        var velDeltaMagnitude = Mathf.Clamp(Vector3.Distance(GetAvgVelocityOfCreature(), cubeForward * MTargetWalkingSpeed), 0, MTargetWalkingSpeed);
        var matchSpeedReward = Mathf.Pow(1 - Mathf.Pow(velDeltaMagnitude / MTargetWalkingSpeed, 2), 2);

        // b. Rotation alignment with target direction.
        //This reward will approach 1 if it faces the target direction perfectly and approach zero as it deviates
        var lookAtTargetReward = (Vector3.Dot(cubeForward, _topTransform.forward) + 1) * 0.5f;

        if (float.IsNaN(lookAtTargetReward) ||
            float.IsNaN(matchSpeedReward)) //throw new ArgumentException($"A reward is NaN. float.");
            //Debug.Log($"matchSpeedReward {Math.Max(matchSpeedReward, 0.1f)} lookAtTargetReward {Math.Max(lookAtTargetReward, 0.1f)}");
        {
            Debug.LogError($"lookAtTargetReward {float.IsNaN(lookAtTargetReward)} or matchSpeedReward {float.IsNaN(matchSpeedReward)}");
        }
        else
        {
            AddReward(Math.Max(matchSpeedReward, 0.1f) * Math.Max(lookAtTargetReward, 0.1f));
        }

        if (resilience_training)
        {
            last_bump += deltaTime;

            if (last_bump > 15)
            {
                if (UnityEngine.Random.Range(0, 1000) < 4)
                {
                    //Debug.Log("Bump after " + (last_bump) + "seconds");
                    last_bump = 0;

                    BodyPart randomBodyPart = _jdController.bodyPartsList[UnityEngine.Random.Range(0, _jdController.bodyPartsList.Count)];
                    randomBodyPart.rb.AddForce(0, 1500, 0, ForceMode.Impulse);
                }
            }
        }
    }
    
    private Vector3 GetNextPathPoint(Vector3 nextPoint)
    {
        if(_timeElapsed >= 1.0f || _path.status == NavMeshPathStatus.PathInvalid)
        {   
            _timeElapsed = 0;
            var oldPath = _path;
            bool pathValid = NavMesh.CalculatePath(_topTransform.position, _target.position, NavMesh.AllAreas, _path);
            if (!pathValid)
            {
                _path = oldPath;
                _walkTargetScript.PlaceTargetCubeRandomly();
                Debug.Log($"Path invalid for {gameObject.name}");
                return nextPoint;
            }
            else
            {
                _pathCornerIndex = 1;
            }
        }
        if(_pathCornerIndex < _path.corners.Length - 1 && Vector3.Distance(_topTransform.position, _path.corners[_pathCornerIndex]) < 4f)
        {
            Debug.Log("Increased path corner index");
            _pathCornerIndex++;
        }
        return _path.corners[_pathCornerIndex];
    }
}