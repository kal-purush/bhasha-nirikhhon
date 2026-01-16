using UnityEngine;
using Unity.VisualScripting;

public class CalculateScoreMultiplier : Unit
{
    [DoNotSerialize]
    public ControlInput inputTrigger;

    [DoNotSerialize]
    public ControlOutput outputTrigger;

    [DoNotSerialize]
    public ValueInput currentTimeLimit;

    [DoNotSerialize]
    public ValueInput timeSinceLastTurn;
    
    [DoNotSerialize]
    public ValueInput scoreScalingFactor;

    [DoNotSerialize]
    public ValueInput turnsChained;

    [DoNotSerialize]
    public ValueOutput scoreMultiplier;

    private float scoreMultiplierValue;

    protected override void Definition()
    {
        inputTrigger = ControlInput("inputTrigger", (flow) => 
        {
            scoreMultiplierValue = flow.GetValue<float>(scoreScalingFactor)*(float)flow.GetValue<int>(turnsChained)*Mathf.Log(flow.GetValue<float>(currentTimeLimit)-flow.GetValue<float>(timeSinceLastTurn)+1)+1;
            return outputTrigger; 
        });
        outputTrigger = ControlOutput("outputTrigger");
        currentTimeLimit = ValueInput<float>("currentTimeLimit", 0);
        timeSinceLastTurn = ValueInput<float>("timeSinceLastTurn", 0);
        scoreScalingFactor = ValueInput<float>("scoreScalingFactor", 0);
        turnsChained = ValueInput<int>("turnsChained", 0);
        scoreMultiplier = ValueOutput<float>("scoreMultiplier", (flow) => scoreMultiplierValue);
    }
}