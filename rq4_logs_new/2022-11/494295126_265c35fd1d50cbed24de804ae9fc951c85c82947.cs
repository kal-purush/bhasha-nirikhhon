using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class CompareWindow : MonoBehaviour, IWindow
{
    public Dropdown left;
    public Dropdown right;

    public SmilesParseEngine leftViewer;
    public SmilesParseEngine rightViewer;

    public GameObject leftViewerSmilesObjectRoot;
    public GameObject rightViewerSmilesObjectRoot;

    private const string DEFAULT_FORMULA = "...";
    private readonly Vector3 LEFT_SMILES_CAMERA_POS = new Vector3(0, 1000, 0);
    private readonly Vector3 RIGHT_SMILES_CAMERA_POS = new Vector3(0, -1000, 0);

    private void Start()
    {
        Init();
    }

    private void Init()
    {
        left.ClearOptions();
        right.ClearOptions();

        left.options.Add(new Dropdown.OptionData(DEFAULT_FORMULA));
        right.options.Add(new Dropdown.OptionData(DEFAULT_FORMULA));

        foreach (string formula in UserDataManager.Instance.savedFormulaList)
        {
            left.options.Add(new Dropdown.OptionData(formula));
            right.options.Add(new Dropdown.OptionData(formula));
        }
    }

    public void OpenViewer(string formula = "")
    {
    }

    public void OnLeftDropdownValueChanged()
    {
        leftViewer.Clear();

        if (left.value == 0)
        {
            leftViewer.transform.parent.gameObject.SetActive(false);
        }
        else
        {
            leftViewer.transform.parent.gameObject.SetActive(true);
            leftViewer.formula = left.options[left.value].text;
            leftViewer.Init();

            leftViewerSmilesObjectRoot.transform.localPosition = LEFT_SMILES_CAMERA_POS;
        }
    }

    public void OnRightDropdownValueChanged()
    {
        rightViewer.Clear();

        if (right.value == 0)
        {
            rightViewer.transform.parent.gameObject.SetActive(false);
        }
        else
        {
            rightViewer.transform.parent.gameObject.SetActive(true);
            rightViewer.formula = right.options[right.value].text;
            rightViewer.Init();

            rightViewerSmilesObjectRoot.transform.localPosition = RIGHT_SMILES_CAMERA_POS;
        }
    }
}