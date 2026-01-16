using UnityEngine;

public class Pitagorasz : MonoBehaviour
{
    [SerializeField] float num1, num2, num3;

    void Start()
    {
        Debug.Log(IsPitagoras(num1, num2, num3));
    }

    bool IsPitagoras(float a, float b, float c)
    {
        if (Mathf.Sqrt(a * a + b * b) == c ||
            Mathf.Sqrt(a * a + c * c) == b ||
            Mathf.Sqrt(c * c + b * b) == a)
            return true;
        return false;
    }
}