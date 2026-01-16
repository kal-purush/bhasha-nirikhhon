using System.Collections.Generic;
using UnityEngine;

public class SelectionController : MonoBehaviour
{
    public static SelectionController Instance { get; private set; }

    public List<Employee> SelectedEmployees { get; } = new List<Employee>();
    
    private void Awake()
    {
        Instance = this;
    }

    public void OnEmployeeClicked(Employee employee)
    {
        if (SelectedEmployees.Contains(employee))
        {
            DeselectEmployee(employee);
        }
        else
        {
            SelectedEmployees.Clear();
            SelectEmployee(employee);
        }
    }

    private void SelectEmployee(Employee employee)
    {
        SelectedEmployees.Add(employee);
        employee.SetSelected(true);
    }

    private void DeselectEmployee(Employee employee)
    {
        SelectedEmployees.Remove(employee);
        employee.SetSelected(false);
    }
}