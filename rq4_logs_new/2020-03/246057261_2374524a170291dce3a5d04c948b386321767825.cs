using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EquipAction : MonoBehaviour {
	//Holding angle is in degrees
	public float holdingAngle;
	public float holdingDistance;
	public float droppingDistance;

	private Entity ownerEntity;

	void Start() {
		ownerEntity = GetComponent<Entity>();
	}

	public void OnEquip(Equipment item, Transform transform) {
        item.ownerEntity = ownerEntity;
        item.transform.parent = transform;
        ownerEntity.equipment = item;

		Rigidbody body = item.GetComponent<Rigidbody>();
        body.isKinematic = true;
        body.useGravity = false;
        body.detectCollisions = false;

        item.transform.position = Calculate.DirectionBasedPosition(this.ownerEntity.position, this.ownerEntity.direction + Vector3.right * this.holdingAngle, this.holdingDistance);
        item.transform.eulerAngles = this.ownerEntity.direction;
        item.transform.gameObject.layer = LayerMask.NameToLayer("Ignore Raycast");
	}

	public void OnDrop(Equipment item) {
        Vector3 dropDirection = new Vector3(0, this.ownerEntity.direction.y, 0);
        item.transform.position = Calculate.DirectionBasedPosition(this.ownerEntity.position, dropDirection, this.droppingDistance);
        item.transform.gameObject.layer = LayerMask.NameToLayer("Item");

        Rigidbody body = item.GetComponent<Rigidbody>();
        body.isKinematic = false;
        body.useGravity = true;
        body.detectCollisions = true;

        item.ownerEntity = null;
        item.transform.parent = null;
        ownerEntity.equipment = null;
    }
}