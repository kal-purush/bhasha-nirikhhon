using UnityEngine;

public class Bullet : MonoBehaviour
{
    [SerializeField] float m_bulletSpeed = 3f;
    [SerializeField] float m_bulletLifeTime = 5f;
    float m_fh;
    float m_fv;

    void Start()
    {
        m_fh = Input.GetAxisRaw("FireHorizontal");
        m_fv = Input.GetAxisRaw("FireVertical");
        Rigidbody m_rb = GetComponent<Rigidbody>();
        m_rb.velocity = new Vector3(m_fh * m_bulletSpeed, m_rb.velocity.y, m_fv * m_bulletSpeed);
        Destroy(this.gameObject, m_bulletLifeTime);
    }
}