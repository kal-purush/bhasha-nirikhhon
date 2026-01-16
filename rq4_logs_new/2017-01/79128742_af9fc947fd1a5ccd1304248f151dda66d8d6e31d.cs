/// <summary>
/// Date:  17.1.24
/// 
/// Description:
/// foreach()
/// message(key) = value
/// send()
/// sender
/// Type or name
/// Content
/// </summary>


using System;
using System.Collections;
using System.Collections.Generic;

namespace SnowFrameWork
{
	public class Message : IEnumerable<KeyValuePair<string, object>>
	{
	
		private Dictionary<string, object> dicDatas = null;   //消息体

		public string Name{ get; private set;}
		public object Sender{ get; private set; }
		public object Content{ get; set;}

		#region 索引器 message[key] = value or data = message[key];
		//索引器
		public object this [string key] {
			get{ 
				if (null == dicDatas || !dicDatas.ContainsKey (key))
					return null;
				return dicDatas [key];
			}
			set{
				if (null == dicDatas)
					dicDatas = new Dictionary<string, object> ();
				if (dicDatas.ContainsKey (key))
					dicDatas [key] = value;
				else
					dicDatas.Add (key, value);
			}
		}
		#endregion

		#region IEnumerable implementation

		public IEnumerator<KeyValuePair<string, object>> GetEnumerator ()
		{
			if (null == dicDatas)
				yield break;
			foreach( KeyValuePair<string,object> kvp in dicDatas)
			{
				yield return kvp;
			}
		}

		#endregion

		#region IEnumerable implementation

		IEnumerator IEnumerable.GetEnumerator ()
		{
			return dicDatas.GetEnumerator();
		}

		#endregion

		#region Message Construction Function
		public Message( string name, object sender)
		{
			Name = name;
			Sender = sender;	
			Content = null;
		}
	
		public Message (string name, object sender, object content)
		{
			Name = name;
			Sender = sender;
			Content = content;
		}

		public Message (string name, object sender, object content , params object[] _dicParams)
		{
			Name = name;
			Sender = sender;
			Content = content;

			if (_dicParams.GetType () == typeof(Dictionary<string, object>)) {
				foreach (object _dicParam in _dicParams) {
					foreach (KeyValuePair<string, object> kvp in _dicParam as Dictionary<string,object> ) {
						//dicDatas [kvp.Key] = kvp.Value;  //error 
						this[kvp.Key] = kvp.Value;  // 索引器
					}
				}
			}
		}

		public Message( Message message)
		{
			Name = message.Name;
			Sender = message.Sender;
			Content = message.Content;

			foreach (KeyValuePair<string, object> kvp in message.dicDatas) {
				this [kvp.Key] = kvp.Value;
			}
		}
		#endregion
	}
}
