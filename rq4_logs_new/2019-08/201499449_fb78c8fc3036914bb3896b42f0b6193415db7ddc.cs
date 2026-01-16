using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;

public class MoneyConsumerController : MonoBehaviour
{
    [SerializeField] private float baseMoneyPerSecond = 104;
    [SerializeField] private List<EmployeeState> baseMoneyActiveStates = new List<EmployeeState>{EmployeeState.Working};

    public Employee Employee { get; set; }

    private readonly List<MoneyGeneratorController> _moneyGenerators = new List<MoneyGeneratorController>();

    public void AddMoneyGeneratorController(MoneyGeneratorController generator)
    {
        _moneyGenerators.Add(generator);
    }

    void Start()
    {
        if (Employee == null)
        {
            throw new InvalidOperationException("MoneyConsumerController has no Employee");
        }
    }


    public float CalculateMoneyGenerated()
    {
        var moneyPerSecond = 0f;

        if (baseMoneyActiveStates.Contains(Employee.State))
        {
            moneyPerSecond += baseMoneyPerSecond;
        }
        
        var moneyGenerators = _moneyGenerators.Where(generator => generator.ActiveStates.Contains(Employee.State))
            .ToList();
        moneyPerSecond += moneyGenerators
                                 .Sum(generator => generator.MoneyPerSecond);
        moneyPerSecond *= moneyGenerators
            .Select(generator => generator.MoneyMultiplierPerSecond)
            .Aggregate(1f, (product, moneyMultiplier) => product * moneyMultiplier);

        return moneyPerSecond;
    }

    private void OnTriggerEnter(Collider other)
    {
        _moneyGenerators.AddRange(other.GetComponents<MoneyGeneratorController>());
    }

    private void OnTriggerExit(Collider other)
    {
        _moneyGenerators.RemoveAll(other.GetComponents<MoneyGeneratorController>().Contains);
    }
}