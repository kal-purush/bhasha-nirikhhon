using UnityEngine;

public class ParticleCreator : Creator<Particle>
{
    private ParticleObjectPool _particleObjectPool;
    private LevelMover _levelMover;

    public ParticleCreator(ParticleObjectPool particleObjectPool, LevelMover levelMover)
    {
        _particleObjectPool = particleObjectPool;
        _levelMover = levelMover;
        _particleObjectPool.Initialize();
    }

    public override Particle Create(Vector3 position)
    {
        var spawnedParticle = _particleObjectPool.Create(position);

        if (!spawnedParticle.IsInitialized)
            spawnedParticle.Inititalize(_levelMover);

        return spawnedParticle;
    }
}