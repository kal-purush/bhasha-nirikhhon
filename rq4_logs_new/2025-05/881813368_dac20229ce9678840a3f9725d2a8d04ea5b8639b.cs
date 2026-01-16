using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EasyUI.Progress;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class InitModuleScanQRView : MonoBehaviour, IModuleView
{
    private ModulePresenter _presenter;

    void Awake()
    {
        _presenter = new ModulePresenter(this, ManagerLocator.Instance.ModuleManager._IModuleService);
    }
    void OnEnable()
    {
        LoadListModule();
    }
    void OnDisable()
    {
    }

    public void LoadListModule()
    {
        _presenter.LoadListModule(1);
    }

    public void DisplayList(List<ModuleInformationModel> models)
    {
        GlobalVariable.temp_Dictionary_ModuleInformationModel = models.ToDictionary(m => m.Name, m => m);
        Debug.Log("DisplayList: " + models.Count);
    }




    private void ShowProgressBar(string title, string details)
    {
        Progress.Show(title, ProgressColor.Blue, true);
        Progress.SetDetailsText(details);
    }
    private void HideProgressBar()
    {
        Progress.Hide();
    }


    public void ShowLoading(string title) => ShowProgressBar(title, "Đang tải dữ liệu...");
    public void HideLoading() => HideProgressBar();
    public void ShowError(string message)
    {


    }
    public void ShowSuccess()
    {
        if (GlobalVariable.APIRequestType.Contains("GET_Module_List"))
        {
            Show_Toast.Instance.ShowToast("success", "Tải danh sách thành công");
        }
        StartCoroutine(Show_Toast.Instance.Set_Instance_Status_False(1f));
    }

    // Không dùng trong ListView
    public void DisplayDetail(ModuleInformationModel model) { }
    public void DisplayCreateResult(bool success) { }
    public void DisplayUpdateResult(bool success) { }
    public void DisplayDeleteResult(bool success) { }

    public void ShowSuccess(string message)
    {
    }


}