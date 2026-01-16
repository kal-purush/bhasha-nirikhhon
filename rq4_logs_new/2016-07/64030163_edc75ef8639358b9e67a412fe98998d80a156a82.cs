using UnityEngine;
using System.Collections;

public class Field : MonoBehaviour {

    [HideInInspector]
    public Position position;

    public GameObject FindPiece () {
        for (int i = 0; i < transform.childCount; i++) {
            if (transform.GetChild(i).GetComponent<Piece>()) {
                return transform.GetChild(i).gameObject;
            }
        }

        return null;
    }

	void OnMouseDown () {
        GameObject piece = FindPiece();
        if (piece) {
            piece.GetComponent<Piece>().OnClick();
        } else {
            transform.parent.GetComponent<Board>().MarkSelected(position);
        }
    }
}