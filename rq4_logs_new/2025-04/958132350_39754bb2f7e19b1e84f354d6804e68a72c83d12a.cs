using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManagerExample : MonoBehaviour
{

    SaveFile saveFile; // Instancia de la clase SaveFile

    SaveLoadSystem saveLoadSystem; // Instancia de la clase SaveLoadSystem
    // Start is called before the first frame update
    void Start()
    {
        // EJEMPLO DE USO DEL SISTEMA DE GUARDADO Y CARGA
        saveLoadSystem = new SaveLoadSystem(); // Inicializa la instancia de SaveLoadSystem
        saveFile = new SaveFile(); // Inicializa la instancia de SaveFile
        saveFile.gameNumber = 1; // Establece el número de juego
        saveFile.playerName = "Jugador1"; // Establece el nombre del jugador
        saveFile.timePlayed = 120.5f; // Establece el tiempo jugado
        saveFile.deathCount = 3; // Establece el número de muertes
        saveFile.stage = 2; // Establece la fase
        saveFile.level = 5; // Establece el nivel

        saveLoadSystem.SaveGame(saveFile); // Llama al método para guardar el juego
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}