using UnityEngine;

public class FicheroGuardado
{
    public int numeroPartida;
    public string nombreJugador;
    public float tiempoJugado; // En segundos
    public int numeroMuertes;
    public int fase; // 1-4
    public int nivel; // Nivel dentro de la fase

    // Método para convertir el objeto en JSON
    public string AJson()
    {
        return JsonUtility.ToJson(this, true); // true para formato legible (pretty print)
    }

    // Método para crear el objeto desde un JSON
    public static FicheroGuardado DesdeJson(string json)
    {
        return JsonUtility.FromJson<FicheroGuardado>(json);
    }
}
