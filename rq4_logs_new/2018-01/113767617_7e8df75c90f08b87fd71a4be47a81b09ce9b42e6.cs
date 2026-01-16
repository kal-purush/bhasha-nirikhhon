using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using UnityEngine.UI;

[RequireComponent(typeof(Dropdown))]
public class MenuDropDown : MonoBehaviour {

    Dropdown _dropdown;
    [SerializeField] ScrollRect _scrollRect;

    [SerializeField] RectTransform[] _contents;

	void Awake () {
        _dropdown = GetComponent<Dropdown>();
        _dropdown.AddOptions(_contents.ToList().Select(c => c.name).ToList());
        _dropdown.onValueChanged.AddListener(delegate { OnValueChanged(); });
	}

    private void OnValueChanged()
    {
        _scrollRect.content.gameObject.SetActive(false);
        _scrollRect.content = _contents[_dropdown.value];
        _scrollRect.content.gameObject.SetActive(true);
    }
}