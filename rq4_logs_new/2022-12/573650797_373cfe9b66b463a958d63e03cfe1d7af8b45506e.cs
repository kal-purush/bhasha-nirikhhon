using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SelectionManager : MonoBehaviour
{   //thanks to infallible code on youtube for the guide
    public string selectableTag = "Selectable";
    public Material selectedMaterial;
    public Material defaultMaterial;

    private Transform _selection;

    // Update is called once per frame
    void Update()
    {
        if(_selection != null){
            var selectionRenderer = _selection.GetComponent<Renderer>();
            selectionRenderer.material = defaultMaterial;
            _selection = null;
        }

        var ray = Camera.main.ScreenPointToRay(Input.mousePosition);
        RaycastHit hitobject;
        if(Physics.Raycast(ray, out hitobject))
        {
            var selection = hitobject.transform;
            if(selection.CompareTag(selectableTag))
            {
                var selectionRenderer = selection.GetComponent<Renderer>();
                if(selectionRenderer != null ){
                    selectionRenderer.material = selectedMaterial;
                }
                _selection = selection;
            }
        }

        
    }
}