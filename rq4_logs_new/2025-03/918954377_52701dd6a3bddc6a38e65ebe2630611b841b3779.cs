using DaftAppleGames.Pooling;
using DaftAppleGames.Utilities;
using UnityEngine;

public class FootstepPoolManager : Singleton<FootstepPoolManager>
{
    [SerializeField] private PrefabPool humanFootstepParticlePool;
    [SerializeField] private PrefabPool humanFootstepDecalPool;

    public PrefabPool HumanFootstepParticlePool => humanFootstepParticlePool;
    public PrefabPool HumanFootstepDecalPool => humanFootstepDecalPool;
}