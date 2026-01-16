using UnityEngine;
using System.Collections;

public class ColocadorDireccion_Triggers : MonoBehaviour {
	void OnTriggerEnter( Collider coll ){
		if (coll.gameObject.name == ControladorEscena.carro.name)
			ControladorEscena.carro.GetComponent<TestDireccion> ().direccionSuelo = this.transform.forward;
	}

}