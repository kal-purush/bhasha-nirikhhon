using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GetParticlePos : MonoBehaviour
{
    // Start is called before the first frame update
    [SerializeField]
    ParticleSystem particle;
    public GameObject Boss;
    BossDonguriMove BossMove;
    public float dist;
    void Start()
    {
        BossMove = Boss.GetComponent<BossDonguriMove>();
    }

    // Update is called once per frame
    void Update()
    {
        if (BossMove.selectAttack == 2 && !BossMove.stopBreath)
        {
            Vector3 pos = this.transform.position;

            var particleCount = particle.particleCount;
            if (particleCount == 0)
            {
                return;
            }

            ParticleSystem.Particle[] particles = new ParticleSystem.Particle[particleCount];
            particle.GetParticles(particles);

            var particleLocalPosition = particles[0].position;
            float posX = particleLocalPosition.x;
            float posZ = particleLocalPosition.z;
            var particleWorldPosition = particle.transform.TransformPoint(particleLocalPosition);

            dist = Mathf.Sqrt(Mathf.Pow(posX - pos.x, 2) + Mathf.Pow(posZ - pos.z, 2));

            Debug.Log($"粒子の座標{dist}");
        }
        else
        {
            dist = 0;
        }

    }
}