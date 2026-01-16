using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using TMPro;
using System.Linq;
using System;
using EasyUI.Progress;
using System.Threading.Tasks;

public class UpdateUIConnectionImagesPanel : MonoBehaviour
{
    [SerializeField] private GameObject General_Panel;
    [SerializeField] private GameObject field_Device_Connection_Panel;
    [SerializeField] private TMP_Text title;
    [SerializeField] private ScrollRect scroll_Area;
    [SerializeField] private GameObject content;
    [SerializeField] private Image connection_ImagePrefab;
    [SerializeField] private GameObject gameObject_Empty;
    [SerializeField] private Button closeButton;

    private readonly List<GameObject> instantiatedImages = new();
    private FieldDeviceInformationModel fieldDeviceInformationModel;
    private List<ImageInformationModel> listConnectionImageInformationModel = new();

    private void OnEnable()
    {
        fieldDeviceInformationModel = GlobalVariable.temp_FieldDeviceInformationModel ?? fieldDeviceInformationModel;
        listConnectionImageInformationModel = fieldDeviceInformationModel?.ListConnectionImages?.ToList() ?? new List<ImageInformationModel>();

        Initialize();
        UpdateTitle();
        UpdateUIListImages();
    }

    private void OnDisable()
    {
        ClearInstantiatedImageObjects();
    }

    private void Initialize()
    {
        title ??= field_Device_Connection_Panel.transform.Find("Title").GetComponent<TMP_Text>();
        scroll_Area ??= field_Device_Connection_Panel.transform.Find("Scroll_Area").GetComponent<ScrollRect>();
        content ??= field_Device_Connection_Panel.transform.Find("Scroll_Area/Content").gameObject;
        connection_ImagePrefab ??= content.transform.Find("Field_Device_Connection_Image_Prefab").GetComponent<Image>();

        closeButton.onClick.RemoveAllListeners();
        closeButton.onClick.AddListener(CloseConnectionPanel);
    }

    private void CloseConnectionPanel()
    {
        General_Panel.SetActive(true);
        field_Device_Connection_Panel.SetActive(false);
    }

    private void UpdateTitle()
    {
        if (fieldDeviceInformationModel != null)
        {
            title.text = $"Sơ đồ kết nối: {fieldDeviceInformationModel.Name}";
        }
    }

    private async void UpdateUIListImages()
    {
        ShowProgressBar("Đang tải hình ảnh...");
        try
        {
            connection_ImagePrefab.gameObject.SetActive(true);

            var tasks = listConnectionImageInformationModel.Select(image =>
            {
                Debug.Log($"image.Name: {image.Name}");
                var newImage = Instantiate(connection_ImagePrefab, content.transform);
                instantiatedImages.Add(newImage.gameObject);
                return LoadImage.Instance.LoadImageFromUrlAsync(image.Name, newImage);
            }).ToList();

            connection_ImagePrefab.gameObject.SetActive(false);
            gameObject_Empty.transform.SetAsLastSibling();

            await Task.WhenAll(tasks);

            foreach (var image in instantiatedImages)
            {
                StartCoroutine(Resize_GameObject_Function.Set_NativeSize_For_GameObject(image.GetComponent<Image>()));
            }

            scroll_Area.verticalNormalizedPosition = 1f;
        }
        catch (Exception ex)
        {
            Debug.LogError($"Error loading images: {ex.Message}");
            Show_Toast.Instance.ShowToast("failure", "Có lỗi xảy ra khi tải hình ảnh");
        }
        finally
        {
            HideProgressBar();
        }
    }

    private void ClearInstantiatedImageObjects()
    {
        foreach (var imageObject in instantiatedImages)
        {
            Destroy(imageObject);
        }
        instantiatedImages.Clear();
    }

    private void ShowProgressBar(string title)
    {
        Progress.Show(title, ProgressColor.Blue, true);
    }

    private void HideProgressBar()
    {
        Progress.Hide();
    }
}