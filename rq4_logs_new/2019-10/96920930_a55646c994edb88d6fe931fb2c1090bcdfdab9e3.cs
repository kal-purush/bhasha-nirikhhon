using System;
using System.IO;
using UnityEngine;
using UnityEngine.UI;

public class PickerController : MonoBehaviour
{
    [SerializeField]
    private FilePickerBehaviour filePicker;

    [SerializeField]
    private GameObject textBinaryAttachmentObject;

    [SerializeField]
    private GameObject pickerBinaryAttachmentObject;

    [SerializeField]
    private InputField textBinaryInput;

    private static string binaryAttachmentPath;
    private static bool usePicker;

    public void Awake()
    {
#if (UNITY_IOS || UNITY_ANDROID) && !UNITY_EDITOR
        pickerBinaryAttachmentObject.SetActive(true);
        usePicker = true;
#else
        pickerBinaryAttachmentObject.SetActive(false);
        usePicker = false;
#endif

        filePicker.Completed += OnFilePicked;
    }

    public void OnPressShowPicker()
    {
        filePicker.Show();
    }

    private void OnFilePicked(string filePath)
    {
        binaryAttachmentPath = filePath;
    }

    private byte[] ParseBytes(string bytesString)

    public static byte[] GetAttachmentBytes()
    {
        if (usePicker && !string.IsNullOrEmpty(binaryAttachmentPath))
        {
            return File.ReadAllBytes(binaryAttachmentPath);
        }

        return ParseBytes(PlayerPrefs.GetString(PuppetAppCenter.BinaryAttachmentKey));
    }
}