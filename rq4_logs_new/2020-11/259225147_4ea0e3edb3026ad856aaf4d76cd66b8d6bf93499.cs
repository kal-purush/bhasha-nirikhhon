using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ArtsColorChange : MonoBehaviour
{
    enum RendererType { Particle, MeshRenderer }
    enum ColorChangeType { One, Two }

    [SerializeField] RendererType rendererType = RendererType.Particle;
    [SerializeField] ColorChangeType changeType = ColorChangeType.Two;

    static readonly string kColorValueName1 = "_Color1";
    static readonly string kColorValueName2 = "_Color2";
    static readonly Color32 kBooleColorCode1 = new Color32(18, 0, 255, 0);
    static readonly Color32 kBooleColorCode2 = new Color32(0, 179, 255, 0);
    static readonly Color32 kRedColorCode1 = new Color32(255, 44, 0, 0);
    static readonly Color32 kRedColorCode2 = new Color32(255, 124, 0, 0);

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

        //敵とプレイヤーでの色替え
        switch (artsStatus.type)
        {
            case ArtsStatus.ParticleType.Player:

                material.SetColor(kColorValueName1, kBooleColorCode1);
                if (changeType != ColorChangeType.One)
                {
                    material.SetColor(kColorValueName2, kBooleColorCode2);
                }

                break;

            case ArtsStatus.ParticleType.Enemy:


                material.SetColor(kColorValueName1, kRedColorCode1);
                if (changeType != ColorChangeType.One)
                {
                    material.SetColor(kColorValueName2, kRedColorCode2);
                }

                break;

            case ArtsStatus.ParticleType.Unknown:break;
            default: break;
        }
    }
}