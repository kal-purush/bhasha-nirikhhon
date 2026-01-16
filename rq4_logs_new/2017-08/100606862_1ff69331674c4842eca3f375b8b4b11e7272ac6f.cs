using UnityEngine;
using UnityEngine.UI;

public class PopupChangePrize : MonoBehaviour {
    public static PrizeStaff prize;
    public InputField input;
	public void BTN_ChangePrize()
    {
        if (Login.debugUser.total_pts >= int.Parse(input.text)) 
        {
            print(" CONSUMIR EL ITEM ID:" + prize.id);
            Login.debugUser.total_pts -= int.Parse(input.text);
            Login.UpdateGUI();
            BTN_ClosePopUp();
        }
        else
        {
            BTN_ClosePopUp();
            Debug.LogError("no tenes puntos");
            NativePlugin.instance.ShowDialog("Puntos Insuficientes!", "No posees la cantidad necesaria de puntos para canjear este producto!","Aceptar", delegate { });
        }

       
    }

    public void BTN_ClosePopUp()
    {
        gameObject.SetActive(false);
    }

}