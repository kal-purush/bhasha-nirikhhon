using UnityEngine;

namespace Assets.Scripts.Ships
{
    public class NavalMothership : NavyBrig
    {
        public Transform InterceptorTransform;
        public int InterceptorsRate = 10;

        protected FrameLocker InterceptorsDebounce = new FrameLocker();

        protected new void Start()
        {
            base.Start();

            TmpDamage = 2;
            InterceptorsDebounce.LockSeconds = InterceptorsRate;
        }

        protected new void Update()
        {
            base.Update();


            if (InterceptorsDebounce.CheckTime())
            {
                SpawnInterceptor();

                InterceptorsDebounce.StartCountdown();
            }

            InterceptorsDebounce.Countdown();
        }

        protected void SpawnInterceptor()
        {
            if (InterceptorTransform)
            {
                Transform obj = Instantiate(InterceptorTransform, transform);

                obj.transform.position += (RandomBool() ? transform.right : -transform.right) * 20;

                obj.LookAt(Vector3.zero);
            }
        }

        protected bool RandomBool()
        {
            return Random.value > 0.5f;
        }
    }
}