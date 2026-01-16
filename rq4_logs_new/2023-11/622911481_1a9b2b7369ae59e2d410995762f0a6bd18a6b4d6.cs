using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MoveTo : MonoBehaviour
{
    [SerializeField] private Vector2 _targetPosition;

    public void Start()
    {
        StartCoroutine(Move());
    }

    private IEnumerator Move()
    {
        float elapsedTime = 0;
        float morphTime = 1f;
        Vector2 startPosition = this.gameObject.GetComponent<Transform>().position;
        while (elapsedTime < morphTime)
        {
            this.gameObject.GetComponent<Transform>().position = Vector2.Lerp(startPosition,
                new Vector2(_targetPosition.x, _targetPosition.y), elapsedTime / morphTime);
            elapsedTime += Time.deltaTime;
            yield return new WaitForSeconds(0.01f);
        }
    }
}