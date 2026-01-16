using System.Collections;
using System.Collections.Generic;
using System.IO;
using UnityEngine;

public class Inventory : MonoBehaviour
{
    Dictionary<int, string> ListaDeProd = new Dictionary<int, string>();
    Dictionary<int, string> ListaDeProdLore = new Dictionary<int, string>();
    Dictionary<int, string> ListaDeNotas = new Dictionary<int, string>();
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public bool ContainsKey(int index)
    {
        return ListaDeProd.ContainsKey(index);
    }

    public void AddToList(string value, int index, CollectableItems type)
    {
        switch (type)
        {
            case CollectableItems.Note:
                ListaDeNotas.Add(index, value);
                break;
            case CollectableItems.Lore:
                ListaDeProdLore.Add(index, value);
                break;
            case CollectableItems.Prod:
                ListaDeProd.Add(index, value);
                break;
        }
        //goListaDeNotas[indexNote].gameObject.SetActive(true);
        
    }
    public Dictionary<int, string> GetListNotes()
    {
        return ListaDeNotas;
    }
    public string GetNote(int index)
    {
        return ListaDeNotas[index];
    }
}