using System.Collections.Generic;
using UnityEngine;

[System.Serializable]
public class UpgradeRequirement
{
    [Tooltip("Nivel al que quieres subir con estos requisitos")]
    public int toLevel; 
    public List<ResourceRequirement> requirements;
}