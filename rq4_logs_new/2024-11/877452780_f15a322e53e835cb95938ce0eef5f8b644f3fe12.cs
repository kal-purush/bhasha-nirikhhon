using UnityEngine;
using Unity.MLAgents;
using Unity.MLAgents.Actuators;
using Unity.MLAgents.Sensors;
public class mlstuff : Agent
{
    public FrameActionBasedAgent agent;
    public GameHandler gameHandler;
    public float reward;
    public GameHandler.State state;


    void Start()
    {
        //print(agent.action);
        gameHandler = FindAnyObjectByType<GameHandler>();
        
    }

    public override void OnEpisodeBegin()
    {
        gameHandler.ResetGame();
    }

    public override void CollectObservations(VectorSensor sensor)
    {
        sensor.AddObservation(state.agentPosition);
        sensor.AddObservation(state.ghostPosition);
        sensor.AddObservation(state.score);
        sensor.AddObservation(state.pelletPositions.Length);
        sensor.AddObservation(state.gameOver);
        //for(int i = 0; i < state.pelletPositions.Length; i++)
        //{
        //sensor.AddObservation(state.pelletPositions[i]);
        //}




    }

    public override void OnActionReceived(ActionBuffers actions)
    {
        agent.action = actions.DiscreteActions[0];
        print("Action: " + agent.action);
    }
    // Start is called once before the first execution of Update after the MonoBehaviour is created
    

    // Update is called once per frame
    void Update()
    {
        reward = gameHandler.currReward;
        state = gameHandler.GetState();
        if(state.gameOver == true || gameHandler.timestep>= 60000)
        {
            AddReward(reward);
            EndEpisode();
        }
        
        //print(state);

    }
}