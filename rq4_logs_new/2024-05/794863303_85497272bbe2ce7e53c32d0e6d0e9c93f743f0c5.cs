using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Unity.VisualScripting;
using UnityEngine;
using UnityEngine.Serialization;
public class SettingPageController : BaseButton
{
    [SerializeField] GameObject _settingParent;
    [SerializeField] GameObject _settingPanel;
    GameObject                  _nowSettingPanel;
    [SerializeField] bool       _defaultOpen = false;
    void Start()
    {
        if (_defaultOpen) { ShowSettingPanel(); }
    }
    protected override void OnClicked() { ShowSettingPanel(); }
    void ShowSettingPanel()
    {
        //settingParent の子オブジェクトをぜんぶ非表示にする
        foreach (Transform child in _settingParent.transform) { child.gameObject.SetActive(false); }
        
        
        
        if (_nowSettingPanel == null) { _nowSettingPanel = Instantiate(_settingPanel, _settingParent.transform); }
        else { _nowSettingPanel.SetActive(true); }
    }
    protected override void OnMouseDown()  { }
    protected override void OnMouseEnter() {  }
    protected override void OnMouseExit()  { }
}