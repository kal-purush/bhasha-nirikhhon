using HoloToolkit.Unity.InputModule;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine.UI;
using UnityEngine.VR.WSA.Input;

namespace Holoworld.InputStates
{
	public class DioramaSetupInputState : InputState
	{
		GestureRecognizer recognizer;
		GameSceneController controller; 

		public DioramaSetupInputState(GameSceneController controller)
		{
			this.controller = controller; 
		}

		public override void Setup()
		{
			recognizer = new GestureRecognizer();
			recognizer.TappedEvent += controller.DioramaSetupInputState_OnTap;
			recognizer.StartCapturingGestures(); 
		}

		public override void Update()
		{
			
		}
		public override void TearDown()
		{
			recognizer.StopCapturingGestures(); 
		}
	}
}