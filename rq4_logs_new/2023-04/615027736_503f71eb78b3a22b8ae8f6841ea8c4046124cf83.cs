using UnityEngine;

//ESTE SCRIPT SE USA EN EL SCRIPT PostProcessingScript AL USAR LA FUNCION GetAspectRatio() EN EL

public static class AspectRatio
{
    public static Vector2 GetAspectRatio(int width, int height)
    {
        float ratio_w = (float)width / (float)height; //(float) castea los valores a un valor decimal
        int ratio_h = 0;

        while (true)
        {
            ratio_h++;
            if (System.Math.Round(ratio_w * ratio_h, 2) == Mathf.RoundToInt(ratio_w * ratio_h)) //Round() redondea el parametro que se le pase como primer parametro y con la cantidad de digitos de precicion que se especifique en el segundo
            {
                break;
            }
        }
        Vector2 acpectRatio = new Vector2((float)System.Math.Round(ratio_w * ratio_h, 2), ratio_h); //Esta variable debe llamarse igual que la variable en el PostProcessingScript que guardara el valor que retorna esta funcion al usarla, en este caso acpectRatio
        return acpectRatio;
    }
}