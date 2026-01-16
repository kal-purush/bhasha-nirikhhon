using UnityEngine;
using System.Collections;
using System.Threading;
using System.Timers;
using System;

public class DireitaEstimulo : Estimulo{
	public GUITexture Seta1Dir;
	public GUITexture Seta2Dir;


	void Start(){
		base.setTextures (Seta1Dir, Seta2Dir);
		
		if (FreqEscolhe.DefFreqManual == true) {
			base.frequencia = FreqEscolhe.FreqDirVal;
		} else {
			frequencia = 9;
		}
		base.Run();
	}
}