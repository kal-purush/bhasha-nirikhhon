using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fall2020_CSC403_Project
{
    public struct Results
    {
        public bool running { get; set; }
        public bool enemyKO { get; set; }

        private bool _lowHP;
        public bool lowHP {
            get{
                return _lowHP;
            }
            set{
                _lowHP = value;
                HealthWarn(_lowHP);
            }
        }

        public void Setup()
        {
            this.running = true;
            this.enemyKO = false;
            this.lowHP = false;
        }
        public event Action<bool> HealthWarn;
    }
}