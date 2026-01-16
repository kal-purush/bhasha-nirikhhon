using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pazaak
{
    public class ScrTypePair
    {
        private int score;
        private int crdType;
        public ScrTypePair(int score, int type)
        {
            this.score = score;
            this.crdType = type;
        }

        public int Score
        {
            get
            {
                return score;
            }

            set
            {
                score = value;
            }
        }

        public int CrdType
        {
            get
            {
                return crdType;
            }

            set
            {
                crdType = value;
            }
        }
    }
}