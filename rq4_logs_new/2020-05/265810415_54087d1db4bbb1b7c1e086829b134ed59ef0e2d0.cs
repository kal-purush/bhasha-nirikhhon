using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace grayzen
{
    using static NativeMethods;

    class Grayscaler
    {
        internal static void GoGrey()
        {
            float redScale = 0.2126f, greenScale = 0.7152f, blueScale = 0.0722f;
            var magEffectInvert = new MAGCOLOREFFECT
            {
                transform = new[] {
                    redScale,   redScale,   redScale,   0.0f,  0.0f,
                    greenScale, greenScale, greenScale, 0.0f,  0.0f,
                    blueScale,  blueScale,  blueScale,  0.0f,  0.0f,
                    0.0f,       0.0f,       0.0f,       1.0f,  0.0f,
                    0.0f,       0.0f,       0.0f,       0.0f,  1.0f
                }
            };

            MagSetFullscreenColorEffect(ref magEffectInvert);
            
        }

        internal static void DontTransformColor()
        {
            var normal = new MAGCOLOREFFECT
            {
                transform = new[] {
                    1.0f,       0.0f,       0.0f,       0.0f,  0.0f,
                    0.0f,       1.0f,       0.0f,       0.0f,  0.0f,
                    0.0f,       0.0f,       1.0f,       0.0f,  0.0f,
                    0.0f,       0.0f,       0.0f,       1.0f,  0.0f,
                    0.0f,       0.0f,       0.0f,       0.0f,  1.0f
                }
            };
            MagSetFullscreenColorEffect(ref normal);
        }
    }
}