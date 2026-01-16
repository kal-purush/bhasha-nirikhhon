using UnityEngine;
using UnityEngine.UI;

public class DebuffNoEyeLeft : TimedBuffer {
private PlayerState playerState;

	protected override void Start() {
		//Si le debuff est deja présent on le supprime. Un joueur ne peut avoir qu'un seul oeil crevé et le timer est infinie.
		playerState = GetComponentInParent<PlayerState>();
		if(playerState != null) {
			DebuffNoEyeLeft[] otherDebuffsLeft = playerState.GetComponentsInChildren<DebuffNoEyeLeft>();
			DebuffNoEyeRight[] otherDebuffsRight = playerState.GetComponentsInChildren<DebuffNoEyeRight>();
			if(otherDebuffsLeft.Length > 1 || otherDebuffsRight.Length > 1){
				Destroy(gameObject);
			}
			else
				base.Start();
		}
	}
	
	protected override void Update() {
		base.Update();
	}
	
	public override void ApplyEffect() {

	}
	
	public override void EndEffect() {
	}
	
	public override string GetTextToDisplay() {
		return "Test de Texte";
	}
}