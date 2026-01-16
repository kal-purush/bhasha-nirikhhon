using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// プレイヤーのArtsと敵のArtsの区別をつける処理
/// </summary>
public class MateriaOutLineControl : MonoBehaviour
{
    enum RendererType { Particle, MeshRenderer }

    [SerializeField] RendererType rendererType = RendererType.Particle;

    // Start is called before the first frame update
    void Start()
    {
        Material material = null;
        ArtsStatus artsStatus = GetComponentInParent<ArtsStatus>();

        //マテリアルの情報取得
        switch (rendererType)
        {
            case RendererType.Particle:
                var ps = GetComponent<ParticleSystem>();
                var psr = ps.GetComponent<ParticleSystemRenderer>();
                material = psr.trailMaterial;
                material = psr.trailMaterial = new Material(material);
                break;

            case RendererType.MeshRenderer:
                var renderer = GetComponent<Renderer>();
                material = renderer.material;
                material = renderer.material = new Material(material);
                break;

            default: break;
        }

        //アウトライン設定
        switch (artsStatus.type)
        {
            case ArtsStatus.ParticleType.Player: material.SetFloat("_IsEnemy", 0); break;
            case ArtsStatus.ParticleType.Enemy: material.SetFloat("_IsEnemy", 1); break;
            case ArtsStatus.ParticleType.Unknown: break;
            default: break;
        }
    }
}