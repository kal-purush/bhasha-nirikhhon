using System;
using UnityEngine;

public class Cube : MonoBehaviour
{
    private static System.Random _rand = new System.Random();
    public event Action<Cube> CubeExploding;
    public event Action<Cube> CubeDisabled;

    public double ShardsChance { get; private set; }

    public void AddForce(Vector3 force)
    {
        GetComponent<Rigidbody>().AddForce(force);
    }

    public void SetShardsChance(double chance)
    {
        ShardsChance = chance;
    }

    public void ChangeColor()
    {
        GetComponent<Renderer>().material.color = new Color((float)_rand.NextDouble(), (float)_rand.NextDouble(), (float)_rand.NextDouble());
    }

    private void Start()
    {
        ChangeColor();
    }

    private void OnDisable()
    {
        CubeDisabled?.Invoke(this);
    }

    private void OnMouseUpAsButton()
    {
        Explode();
    }

    private void Explode()
    {
        CubeExploding?.Invoke(this);
        Destroy(gameObject);
    }
}