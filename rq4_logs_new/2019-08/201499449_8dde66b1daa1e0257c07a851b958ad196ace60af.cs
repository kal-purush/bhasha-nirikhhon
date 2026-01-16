using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EmployeeSpawner : MonoBehaviour
{
    [SerializeField] private Employee employeePrefab;
    [SerializeField] private Transform spawnLocation;
    [SerializeField] private float minStressLevel = 10;
    [SerializeField] private float maxStressLevel = 40;
    
    private IEnumerable<WorkStation> workStations;
//    private List<Employee> employees = new List<Employee>();
    
    private void Awake()
    {
        workStations = FindObjectsOfType<WorkStation>();
        StartCoroutine(SpawnEmployees());
    }

    // Start of day
    private IEnumerator SpawnEmployees()
    {
        foreach (var workStation in workStations)
        {
            SpawnEmployee(workStation);
            yield return new WaitForSeconds(1f);
        }
    }

    // Spawn at door
    private void SpawnEmployee(WorkStation assignedWorkstation)
    {
        var employee = Instantiate(employeePrefab, spawnLocation.position, Quaternion.identity);
        employee.AssignWorkStation(assignedWorkstation);
        employee.MoveToWorkStation();

        float baseStress = Random.Range(minStressLevel, maxStressLevel);
        employee.StressConsumerController.SetBaseStress(baseStress);
    }
}