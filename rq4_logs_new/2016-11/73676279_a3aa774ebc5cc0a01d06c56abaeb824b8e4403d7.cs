using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SecurityGuard
{
    public interface IAttack
    {
        string key1 { get; set; }
        string key2 { get; set; }
        string key3 { get; set; }
        DateTime attackedtime { get; set; }
        DateTime lastime { get; set; }
        int times { get; set; }
        bool attacking();
    }
    public interface IAvoidAttacker
    {
        bool Attacking(string key1, string key2, string key3);

        int MaxAttackingTimes { get; set; }
        int MinPendingMinutes { get; set; }
        int AttackedIntervalTimes { get; set; }
        void Init(string configkey);
        void Init(IAvoidAttacker attacker);
        void Init();
        string Tip();
    }
    public class Attack : IAttack
    {
        public Attack() { }
        public Attack(string key1, string key2, string key3)
        {
            this.key1 = key1;
            this.key2 = key2;
            this.key3 = key3;
            attackedtime = lastime = DateTime.Now;
            times = 1;
        }
        public string key1 { get; set; }

        public string key2 { get; set; }

        public string key3 { get; set; }

        public int times { get; set; }

        public DateTime attackedtime { get; set; }
        public DateTime lastime { get; set; }

        public bool attacking()
        {
            if (lastime.AddMinutes(AvoidIPAttacker.Instance.MinPendingMinutes) < DateTime.Now)
            {
                times = 1;
                lastime = attackedtime = DateTime.Now;
                return false;
            }
            if (times < AvoidIPAttacker.Instance.MaxAttackingTimes)
            {
                times++;
                lastime = DateTime.Now;
                return false;
            }
            times++;
            lastime = DateTime.Now;
            return true;
        }


    }
    public class AvoidIPAttacker : IAvoidAttacker
    {
        public AvoidIPAttacker()
        {
            AttackedIntervalTimes = 10;
            MaxAttackingTimes = 10;
            MinPendingMinutes = 60;
            _lastrefreshime = DateTime.Now;
        }
        public string Tip()
        {
            string msg = "";
            if (MinPendingMinutes < 60)
                msg = string.Format("您尝试的登录次数超过{0}次,请{1}分钟后再试", MaxAttackingTimes, MinPendingMinutes);
            else if (MinPendingMinutes % 60 == 0)
                msg = string.Format("您尝试的登录次数超过{0}次,请{1}小时后再试", MaxAttackingTimes, MinPendingMinutes / 60);
            else
                msg = string.Format("您尝试的登录次数超过{0}次,请{1}小时{2}分钟后再试", MaxAttackingTimes, MinPendingMinutes / 60, MinPendingMinutes % 60);
            return msg;

        }
        static IAvoidAttacker _instance = new AvoidIPAttacker();
        public static IAvoidAttacker Instance { get { return _instance; } }

        public static List<IAttack> _attacks = new List<IAttack>();

        public bool Attacking(string key1, string key2 = null, string key3 = null)
        {
            lock (_locker1)
            {
                Init();
                Refresh();
                if (string.IsNullOrWhiteSpace(key1)) return false;
                if (string.IsNullOrWhiteSpace(key2) && string.IsNullOrWhiteSpace(key3)) return false;
                IAttack attack = null;
                if (key3 == null && key2 == null)
                    attack = _attacks.SingleOrDefault(a => a.key1 == key1);
                else if (key3 == null)
                    attack = _attacks.SingleOrDefault(a => a.key1 == key1 && a.key2 == key2);
                else attack = _attacks.SingleOrDefault(a => a.key1 == key1 && a.key2 == key2 && a.key3 == key3);
                if (attack == null)
                {
                    _attacks.Add(new Attack(key1, key2, key3));
                    return false;
                }
                return attack.attacking();
            }
        }

        public DateTime _lastrefreshime { get; set; }
        static object _locker = new object();
        static object _locker1 = new object();
        public void Refresh()
        {
            lock (_locker)
            {
                if (_lastrefreshime.AddMinutes(MinPendingMinutes) > DateTime.Now)
                {
                    _attacks.RemoveAll(a => a.lastime.AddMinutes(MinPendingMinutes) < DateTime.Now);
                    _lastrefreshime = DateTime.Now;
                }
            }
        }

        public int MaxAttackingTimes { get; set; }

        public int MinPendingMinutes { get; set; }

        public int AttackedIntervalTimes { get; set; }


        public void Init(string configkey = "AvoidAttackingConfig")
        {
            string mt = System.Configuration.ConfigurationManager.AppSettings.Get(configkey);
            if (string.IsNullOrWhiteSpace(mt))
                return;

            var keys = mt.Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
            if (keys == null || keys.Length != 3) return;

            int v = 0;
            if (int.TryParse(keys[0], out v))
            {
                AttackedIntervalTimes = v;
            }
            v = 0;
            if (int.TryParse(keys[1], out v))
            {
                MaxAttackingTimes = v;
            }
            v = 0;
            if (int.TryParse(keys[2], out v))
            {
                MinPendingMinutes = v;
            }
        }

        public void Init()
        {

            string mt = System.Configuration.ConfigurationManager.AppSettings.Get("AttackingAvoidMaxPendingTimes");
            if (string.IsNullOrWhiteSpace(mt))
                return;

            var keys = mt.Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
            if (keys == null || keys.Length != 3) return;

            int v = 0;
            if (int.TryParse(keys[0], out v))
            {
                AttackedIntervalTimes = v;
            }
            v = 0;
            if (int.TryParse(keys[1], out v))
            {
                MaxAttackingTimes = v;
            }
            v = 0;
            if (int.TryParse(keys[2], out v))
            {
                MinPendingMinutes = v;
            }
        }


        public void Init(IAvoidAttacker attacker)
        {
            _instance = attacker;
        }
    }
}