using UnityEngine;

public class Bomb : MonoBehaviour
{
    int m_bombCount = 50;
    [SerializeField] GameObject m_bombBullet;
    Vector3 tmp;

    void Start()
    {
        tmp = this.transform.position;
    }
    private void OnTriggerEnter(Collider other)
    {
        if (other.CompareTag("Bullet"))
        {
            for (int i = 0; i < m_bombCount; i++)
            {
                Instantiate(m_bombBullet, new Vector3(tmp.x, tmp.y, tmp.z), this.transform.rotation);
            }
        }
        Destroy(this.gameObject);
    }
}