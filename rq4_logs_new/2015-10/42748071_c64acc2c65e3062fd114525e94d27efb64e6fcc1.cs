using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using System.Xml.Serialization;
using System.Xml;
using System.IO;

public class WaveCreator{

	
	public List<Wave> DeserializeXml(string path){

		XmlSerializer serializer = new XmlSerializer (typeof(WaveListXml));
		FileStream stream = new FileStream (path, FileMode.Open);
		WaveListXml wxList = serializer.Deserialize (stream) as WaveListXml;
		stream.Close ();


		List<Wave> retList = new List<Wave> ();
		foreach (WaveXml wx in wxList.waves) {
			Debug.Log (wx);

			List<GameObject> listGo = new List<GameObject>(); 
			foreach(string s in wx.enemiesName){
				Debug.Log (s);
				GameObject go = (GameObject) Resources.Load("Prefabs/" + s);
				listGo.Add(go);

			}
			Wave w = new Wave(listGo, wx.timer);

			retList.Add(w);
		}

		return retList;
	}

}