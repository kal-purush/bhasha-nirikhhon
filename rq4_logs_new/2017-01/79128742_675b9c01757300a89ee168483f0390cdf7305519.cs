using System;
using UnityEngine;

namespace SnowFrameWork
{
    public abstract class Singleton<T> where T: class, new()
    {
        protected static T _instance;

        public static T Instance {
            get { 
                if( null == _instance )
                {
                    _instance = new T();
                }
                return _instance;
            }
        }
        protected Singleton()
        {
            if (null != _instance)
            {
				throw new SingletonException ( "Exception:this "+typeof(T).ToString() + "Singleton instance is no real" );
            }
			Init ();
        }

		//commits snow.xiao@2017.1.17
		//因为父类的构造函数是私有的（protected),所以为了提供初始化方法
		//为其提供了公有的初始化方法 Init()
		//commits
		public virtual void Init()
        {
            Debug.Log("Singleton<T> : Init");
        }
    }
}