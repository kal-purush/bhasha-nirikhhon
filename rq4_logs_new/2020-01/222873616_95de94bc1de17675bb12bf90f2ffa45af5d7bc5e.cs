using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Rendering.PostProcessing;

namespace HungraviyEx2019
{
    public class PPSEnabler : MonoBehaviour
    {
#if UNITY_ANDROID
        PostProcessLayer ppl;

        private void Awake()
        {
            ppl = GetComponent<PostProcessLayer>();
            string st = SystemInfo.graphicsDeviceType.ToString().ToLower();
            if (st != "opengles2")
            {
                if (ppl != null)
                {
                    ppl.enabled = true;
                }
                Graphics.activeTier = UnityEngine.Rendering.GraphicsTier.Tier3;
            }
            else
            {
                if (ppl != null)
                {
                    ppl.enabled = false;
                }
                Graphics.activeTier = UnityEngine.Rendering.GraphicsTier.Tier1;
            }
        }
#endif
    }
}