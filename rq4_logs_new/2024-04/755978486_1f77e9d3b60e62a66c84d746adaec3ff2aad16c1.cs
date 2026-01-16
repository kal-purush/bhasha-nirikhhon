using UnityEngine;

public class BossPuppeteerFightManager : MonoBehaviour
{
    [SerializeField] private GameObject[] _doors;

    [SerializeField] private GameObject[] _activeGameObjectsAfterFight;
    [SerializeField] private GameObject[] _deactiveGameObjectsAfterFight;

    [SerializeField] private GameObject _boss;

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision != null && collision.gameObject == PlayerManager.Instance.Player)
        {
            foreach (GameObject go in _doors)
            {
                go.SetActive(true);
            }
        }

    }

    private void Update()
    {
        if (_boss == null)
        {
            foreach (GameObject go in _doors)
            {
                go.SetActive(false);
            }
            foreach (GameObject go in _activeGameObjectsAfterFight)
            {
                go.SetActive(true);
            }
            foreach (GameObject go in _deactiveGameObjectsAfterFight)
            {
                go.SetActive(false);
            }
        }
    }
}