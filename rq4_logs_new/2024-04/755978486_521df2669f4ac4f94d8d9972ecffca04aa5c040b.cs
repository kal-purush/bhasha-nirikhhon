using Assets.Scripts.Interactables;
using UnityEngine;

public class OnDeath : MonoBehaviour
{
    public GameObject CurrencyPrefab;
    public int ValuePerCurrency;
    public int CurrencyAmount;
    public float velocity;

    public void Activate()
    {
        for (int i = 0; i < CurrencyAmount; i++)
        {
            GameObject go = Instantiate(CurrencyPrefab, gameObject.transform.position, Quaternion.identity);
            if (go.TryGetComponent(out Currency c)) c.SetValue(ValuePerCurrency);

            if (go.TryGetComponent(out Rigidbody2D rb))
            {
                float facingAngle = Random.value * 2 * Mathf.PI;
                rb.velocity = new Vector2(Mathf.Sin(facingAngle) * velocity, Mathf.Cos(facingAngle) * velocity);
            }

        }
    }
}