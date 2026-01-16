using System;
using System.Collections.Generic;
using System.Linq;
using Sim;
using UnityEngine;
using UnityEngine.UI;

public class ShopListView : MonoBehaviour {
    [Header("Settings")]
    [SerializeField]
    private GameObject itemListContainer;
    
    [SerializeField]
    private VerticalLayoutGroup verticalLayoutGroup;

    [SerializeField]
    private PhoneArticleCardUI articleCardPrefab;

    private List<PhoneArticleCardUI> items = new List<PhoneArticleCardUI>();
    
    public void Setup(ShopCategoryConfig config) {
        if (items.Count > 0) {
            foreach (var item in items) {
                Destroy(item.gameObject);
            }

            items.Clear();
        }
        
        DatabaseManager.PropsDatabase.GetProps().Where(x => x.GetPropsType().Equals(config.PropsType) && x.IsBuyable()).ToList().ForEach(x => {
            PhoneArticleCardUI card = Instantiate(this.articleCardPrefab, this.verticalLayoutGroup.transform);
            card.Setup(x);
            items.Add(card);
        });
        
        this.Show();
    }

    public void Filter(string itemName) {
        this.items.ForEach(item => {
           item.gameObject.SetActive(item.PropsConfig.GetDisplayName().ToLower().IndexOf(itemName.ToLower(), StringComparison.Ordinal) != -1); 
        });
    }

    public void Show() {
        this.itemListContainer.SetActive(true);
    }

    public void Hide() {
        this.itemListContainer.SetActive(false);
    }
}