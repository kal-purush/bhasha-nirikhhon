using UnityEngine;
using System.Threading.Tasks;


public class GraphicsSettings : MonoBehaviour
{
    public void SetLowGraphics()
    {
        QualitySettings.SetQualityLevel(0, true);
        Debug.Log("Graphics set to Low");
    }

    public void SetMediumGraphics()
    {
        QualitySettings.SetQualityLevel(2, true);
        Debug.Log("Graphics set to Medium");
    }

    public void SetHighGraphics()
    {
        QualitySettings.SetQualityLevel(5, true);
        Debug.Log("Graphics set to High");
    }
    
    
    public async void ActivateAndDeactivate(GameObject obj)
    {
        obj.SetActive(true);
        await Task.Delay(1000); // 1000 milliseconds = 1 second
        obj.SetActive(false);
    }
    
    
}