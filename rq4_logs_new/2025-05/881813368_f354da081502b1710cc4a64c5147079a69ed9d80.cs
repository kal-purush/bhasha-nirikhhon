using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EasyUI.Progress;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class UpdateUISpecificationPanel : MonoBehaviour, IModuleSpecificationView, IAdapterSpecificationView
{
    private ModuleSpecificationPresenter _moduleSpecificationPresenter;
    private AdapterSpecificationPresenter _adapterSpecificationPresenter;
    private ModuleSpecificationModel moduleSpecificationModel;
    private AdapterSpecificationModel adapterSpecificationModel;
    private int moduleSpecificationId;
    private int adapterSpecificationId;
    public Press_PDF_Open_Online_Catalog press_PDF_Open_Online_Catalogue;
    [Header("Module Specification")]
    public TMP_Text module_Specification_Title;
    public GameObject[] module_Spe_values;
    public TMP_Text module_CodeValue;
    public TMP_Text module_TypeValue;
    public TMP_Text module_NumOfIOValue;
    public TMP_Text module_SignalTypeValue;
    public TMP_Text module_CompatibleTBUsValue;
    public TMP_Text module_OperatingVoltageValue;
    public TMP_Text module_OperatingCurrentValue;
    public TMP_Text module_FlexbusCurrentValue;
    public TMP_Text module_AlarmValue;
    public TMP_Text module_NoteValue;
    public string module_PDFManualValue;
    public Button module_PDFManualButton;

    [Header("Adapter Specification")]
    public TMP_Text adapter_Specification_Title;
    public GameObject[] adapter_Spe_values;
    public TMP_Text adapter_CodeValue;
    public TMP_Text adapter_TypeValue;
    public TMP_Text adapter_CommunicationValue;
    public TMP_Text adapter_NumOfAdapterAllowedValue;
    public TMP_Text adapter_CommSpeedValue;
    public TMP_Text adapter_InputSupplyValue;
    public TMP_Text adapter_OutputSupplyValue;
    public TMP_Text adapter_InrushCurrentValue;
    public TMP_Text adapter_AlarmValue;
    public TMP_Text adapter_NoteValue;
    public string adapter_PDFManualValue;
    public Button adapter_PDFManualButton;
    void Awake()
    {
        _moduleSpecificationPresenter = new ModuleSpecificationPresenter(this, ManagerLocator.Instance.ModuleSpecificationManager._IModuleSpecificationService);
        _adapterSpecificationPresenter = new AdapterSpecificationPresenter(this, ManagerLocator.Instance.AdapterSpecificationManager._IAdapterSpecificationService);
    }
    void OnEnable()
    {
        StopAllCoroutines();
        LoadSpecificationData();
    }
    void OnDisable()
    {
    }
    private void LoadSpecificationData()
    {
        StartCoroutine(LoadModuleSpecificationData());
        StartCoroutine(LoadAdapterSpecificationData());
    }
    private IEnumerator LoadModuleSpecificationData()
    {
        yield return null;
        if (GlobalVariable.moduleSpecificationId != 0)
        {
            foreach (var gameObject in module_Spe_values)
            {
                gameObject.SetActive(true);
            }
            moduleSpecificationId = GlobalVariable.moduleSpecificationId;
            _moduleSpecificationPresenter.LoadDetailById(moduleSpecificationId);
        }
        else
        {
            foreach (var gameObject in module_Spe_values)
            {
                gameObject.SetActive(false);
            }
            module_Specification_Title.text = "Chưa cập nhật";
            module_Specification_Title.fontWeight = FontWeight.Bold;
            module_Specification_Title.color = Color.red;
        }
    }
    private IEnumerator LoadAdapterSpecificationData()
    {
        yield return null;
        if (GlobalVariable.adapterSpecificationId != 0)
        {
            foreach (var gameObject in adapter_Spe_values)
            {
                gameObject.SetActive(true);
            }
            adapterSpecificationId = GlobalVariable.adapterSpecificationId;
            _adapterSpecificationPresenter.LoadDetailById(adapterSpecificationId);
        }
        else
        {
            foreach (var gameObject in adapter_Spe_values)
            {
                gameObject.SetActive(false);
            }
            adapter_Specification_Title.text = "Chưa cập nhật";
            adapter_Specification_Title.fontWeight = FontWeight.Bold;
            adapter_Specification_Title.color = Color.red;
            Debug.Log("Module Specification ID or Adapter Specification ID is not set.");
        }
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
        if (GlobalVariable.APIRequestType.Contains("GET_ModuleSpecification")
        || GlobalVariable.APIRequestType.Contains("GET_AdapterSpecification"))
        {
            Show_Toast.Instance.ShowToast("failure", "Tải dữ liệu thất bại");

        }
        StartCoroutine(Show_Toast.Instance.Set_Instance_Status_False(1f));
    }
    public void ShowSuccess(string message)
    {
        if (GlobalVariable.APIRequestType.Contains("GET_ModuleSpecification") ||
            GlobalVariable.APIRequestType.Contains("GET_AdapterSpecification"))
        {
            Show_Toast.Instance.ShowToast("success", "Tải dữ liệu thành công");
        }
        StartCoroutine(Show_Toast.Instance.Set_Instance_Status_False(1f));
    }
    private IEnumerator UpdateModuleSpecificationUI()
    {
        yield return new WaitUntil(() => moduleSpecificationModel != null && adapterSpecificationModel != null);
        if (moduleSpecificationModel != null)
        {
            module_CodeValue.text = moduleSpecificationModel.Code;
            module_TypeValue.text = moduleSpecificationModel.Type;
            module_NumOfIOValue.text = moduleSpecificationModel.NumOfIO;
            module_SignalTypeValue.text = moduleSpecificationModel.SignalType;
            module_CompatibleTBUsValue.text = moduleSpecificationModel.CompatibleTBUs;
            module_OperatingVoltageValue.text = moduleSpecificationModel.OperatingVoltage;
            module_OperatingCurrentValue.text = moduleSpecificationModel.OperatingCurrent;
            module_FlexbusCurrentValue.text = moduleSpecificationModel.FlexbusCurrent;
            module_AlarmValue.text = moduleSpecificationModel.Alarm;
            module_NoteValue.text = moduleSpecificationModel.Note;
            module_PDFManualValue = moduleSpecificationModel.PdfManual;
            press_PDF_Open_Online_Catalogue.listButton.Add(module_PDFManualButton);
            press_PDF_Open_Online_Catalogue.online_Module_Catalog_Url.Add(moduleSpecificationModel.Code, moduleSpecificationModel.PdfManual);
            module_PDFManualButton.onClick.RemoveAllListeners();
            module_PDFManualButton.onClick.AddListener(() =>
            {
                press_PDF_Open_Online_Catalogue.Open_url(moduleSpecificationModel.Code);
            });

            Debug.Log("Module Specification Data Loaded");
        }
        else
        {
            Debug.Log("Module Specification Data is null");
        }
    }
    private IEnumerator UpdateAdapterSpecificationUI()
    {
        yield return new WaitUntil(() => moduleSpecificationModel != null && adapterSpecificationModel != null);
        if (adapterSpecificationModel != null)
        {
            adapter_CodeValue.text = adapterSpecificationModel.Code;
            adapter_TypeValue.text = adapterSpecificationModel.Type;
            adapter_CommunicationValue.text = adapterSpecificationModel.Communication;
            adapter_NumOfAdapterAllowedValue.text = adapterSpecificationModel.NumOfModulesAllowed;
            adapter_CommSpeedValue.text = adapterSpecificationModel.CommSpeed;
            adapter_InputSupplyValue.text = adapterSpecificationModel.InputSupply;
            adapter_OutputSupplyValue.text = adapterSpecificationModel.OutputSupply;
            adapter_InrushCurrentValue.text = adapterSpecificationModel.InrushCurrent;
            adapter_AlarmValue.text = adapterSpecificationModel.Alarm;
            adapter_NoteValue.text = adapterSpecificationModel.Note;

            adapter_PDFManualValue = adapterSpecificationModel.PdfManual;
            press_PDF_Open_Online_Catalogue.listButton.Add(adapter_PDFManualButton);
            press_PDF_Open_Online_Catalogue.online_Adapter_Catalog_Url.Add(adapterSpecificationModel.Code, adapterSpecificationModel.PdfManual);
            adapter_PDFManualButton.onClick.RemoveAllListeners();
            adapter_PDFManualButton.onClick.AddListener(() =>
            {
                press_PDF_Open_Online_Catalogue.Open_url(adapterSpecificationModel.Code);
            });

            // Debug.Log("Adapter PDF Manual: " + adapter_PDFManualValue);
            Debug.Log("Adapter Specification Data Loaded");
        }
        else
        {
            Debug.Log("Adapter Specification Data is null");
        }
    }
    // Không dùng trong ListView
    public void DisplayCreateResult(bool success) { }
    public void DisplayUpdateResult(bool success) { }
    public void DisplayDeleteResult(bool success) { }

    public void DisplayList(List<ModuleSpecificationModel> models)
    {
    }

    public void DisplayDetail(ModuleSpecificationModel model)
    {
        if (model != null)
        {
            moduleSpecificationModel = model;
            StartCoroutine(UpdateModuleSpecificationUI());
        }

    }

    public void DisplayList(List<AdapterSpecificationModel> models)
    {
    }

    public void DisplayDetail(AdapterSpecificationModel model)
    {
        if (model != null)
        {
            adapterSpecificationModel = model;
            StartCoroutine(UpdateAdapterSpecificationUI());
        }
    }


    // public void DisplayList(List<ModuleInformationModel> models)
    // {
    //     throw new NotImplementedException();
    // }

    // public void DisplayDetail(ModuleInformationModel model)
    // {
    //     throw new NotImplementedException();
    // }
}