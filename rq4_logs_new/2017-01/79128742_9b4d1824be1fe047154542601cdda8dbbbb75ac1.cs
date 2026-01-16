using System;
using UnityEngine;

namespace SnowFrameWork
{
	static public class MethodExtension
	{
		static public T GetOrAddComponent<T>( this GameObject go ) where T : UnityEngine.Component
		{
			T ret = go.GetComponent<T> ();
			if (null == ret) 
				ret = go.AddComponent<T> ();
			return ret;
		}

	}
}
