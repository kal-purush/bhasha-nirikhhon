using UnityEngine;
using UnityEngine.UIElements;

namespace DataskopAR {

	public class MinimapUI : MonoBehaviour {

#region Fields

		[SerializeField] private UIDocument minimapDocument;

#endregion

#region Properties

		private VisualElement MinimapRoot { get; set; }

#endregion

#region Methods

		private void Awake() {
			MinimapRoot = minimapDocument.rootVisualElement;
		}

		public void OnCalibrationFinished() {
			MinimapRoot.visible = true;
		}

		public void OnCalibrationInitialized() {
			MinimapRoot.visible = false;
		}

#endregion

	}

}