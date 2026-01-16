using System.Collections;
using System.Collections.Generic;
using DefaultNamespace.Battle;
using UnityEngine;
using UnityEngine.UI;

public class UndoLoot : MonoBehaviour
{
    public Loot sLoot;
    public GameObject prefabLoot;
    public Button btn;
    private LootBoxPainel _painel;
    private PlayerMove _player;
    
    // Start is called before the first frame update
    void Start()
    {
        _player = FindObjectOfType<PlayerMove>();
        _painel = FindObjectOfType<LootBoxPainel>();
        btn.onClick.AddListener(UndoL);
        btn.gameObject.SetActive(false);
    }

    private void UndoL()
    {
        _player.CancelLoot();
        var l = Instantiate(prefabLoot, Vector3.zero, Quaternion.identity, _painel.transform).GetComponent<Loot>();
        l.SetValue(sLoot.CanUse,sLoot._rarit,sLoot.TypeLoot);
        btn.gameObject.SetActive(false);
    }
    
}