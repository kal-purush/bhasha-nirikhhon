using UnityEngine;
using UnityEngine.UI;

public class EmployeeInfo : MonoBehaviour
{
    [SerializeField] private Employee employee;
    [SerializeField] private Text nameText;
    [SerializeField] private Text stressLevelText;
    [SerializeField] private Text stateText;

    private void Awake()
    {
        nameText.text = employee.EmployeeName;
    }

    private void Update()
    {
        float stressAsPercentage = employee.PercentageStressLevel * 100;
        stressLevelText.text = string.Format("{0:#}% stressed", stressAsPercentage);
        stateText.text = GetStateText(employee.State);
    }

    private string GetStateText(EmployeeState state)
    {
        switch (state)
        {
            case EmployeeState.Walking:
                return "Walking to..."; // TODO return workstation or break location name
            case EmployeeState.Working:
                return "Working";
            case EmployeeState.Break:
                return "Taking a break";
            case EmployeeState.OverStressed:
                return "About to explode";
        }
        return string.Empty;
    }

    public void SetVisible(bool visible)
    {
        gameObject.SetActive(visible);
    }
}