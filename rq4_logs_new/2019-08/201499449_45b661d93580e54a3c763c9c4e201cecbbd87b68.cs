using System.Collections.Generic;
using System.Linq;
using UnityEngine;

public class StressConsumerController : MonoBehaviour
{
    [SerializeField] private float stressLevel = 50;
    [SerializeField] private float stressThreshold = 100;

    public Employee Employee { get; set; }

    private readonly List<StressGeneratorController> _stressGenerators = new List<StressGeneratorController>();

    public float PercentageStressLevel => stressLevel / stressThreshold;


    void Update()
    {
        ApplyStressPerSecond();
    }

    public bool IsOverstressed()
    {
        return stressLevel >= stressThreshold;
    }

    private void ApplyStressPerSecond()
    {
        // only consider generators with the correct state
        var stressGenerators = _stressGenerators.Where(generator => generator.ActiveStates.Contains(Employee.State))
            .ToList();
        // sum up fixed stress per second
        var stressPerSecond = stressGenerators
            .Sum(generator => generator.StressPerSecond);
        // multiply with product of all stress multipliers
        stressPerSecond *= stressGenerators
            .Select(generator => generator.StressMultiplierPerSecond)
            .Aggregate(1, (product, stressMultiplier) => product * stressMultiplier);

        // apply to stress level
        stressLevel += stressPerSecond * Time.deltaTime;
    }

    private void OnTriggerEnter(Collider other)
    {
        var newStressGenerators = other.GetComponents<StressGeneratorController>();
        _stressGenerators.AddRange(newStressGenerators);
        ApplyStressFixed(newStressGenerators);
    }

    private void ApplyStressFixed(StressGeneratorController[] newStressGenerators)
    {
        foreach (StressGeneratorController stressGenerator in newStressGenerators)
        {
            stressLevel += stressGenerator.StressFixed;
        }
    }

    private void OnTriggerExit(Collider other)
    {
        _stressGenerators.RemoveAll(other.GetComponents<StressGeneratorController>().Contains);
    }
}