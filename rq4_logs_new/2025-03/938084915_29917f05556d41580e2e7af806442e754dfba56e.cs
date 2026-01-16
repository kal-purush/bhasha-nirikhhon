using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jelado_proj
{
    public enum ElteresTipusa {
        SEMMI = 0,
        IDO = 1,
        KOORDINATA = 2
    }

    class Elteres
    {
        public Jelado kovetkezoJel;
        public int kimaradtJelekSzama;
        public ElteresTipusa elteresTipusa;

        public Elteres(Jelado kovetkezoJel, int kimaradtJelekSzama, ElteresTipusa elteresTipusa)
        {
            this.kovetkezoJel = kovetkezoJel;
            this.kimaradtJelekSzama = kimaradtJelekSzama;
            this.elteresTipusa = elteresTipusa;
        }
    }
}