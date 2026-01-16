using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class placeInMap : MonoBehaviour
{
    public float horizontalSpaceMod;
    public TextAsset mapFile;
    public GameObject hitboxPrefab;
    private string map = "0";

    // Start is called before the first frame update
    void Start() {
        map = mapFile.text; // read from txt file
        for (int i = 0; i < map.Length; i++) {
            int mapCode = int.Parse(map[i].ToString());
            switch (mapCode) {
                case 0:
                    break;
                case 1: // bottom
                    Instantiate(hitboxPrefab, transform) // instantiate new hitbox and set it's position
                        .transform.position = new Vector3(i * horizontalSpaceMod, 1.0f, 0.0f);
                    break;
                case 2: // top
                    Instantiate(hitboxPrefab, transform) // instantiate new hitbox and set it's position
                        .transform.position = new Vector3(i * horizontalSpaceMod, 2.0f, 0.0f);
                    break;
                case 3: // both
                    Instantiate(hitboxPrefab, transform) // instantiate new hitbox and set it's position
                        .transform.position = new Vector3(i * horizontalSpaceMod, 1.0f, 0.0f);
                    Instantiate(hitboxPrefab, transform) // instantiate new hitbox and set it's position
                        .transform.position = new Vector3(i * horizontalSpaceMod, 2.0f, 0.0f);
                    break;
            }
        }
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}