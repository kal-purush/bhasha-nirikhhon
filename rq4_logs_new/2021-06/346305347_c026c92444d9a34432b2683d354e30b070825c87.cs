using System.Collections;
using UnityEngine;

public class TutorialManager : MonoBehaviour
{
    private readonly string[] intro = {
        "Welcome to DAY ONE at Cosmos X",
        "Left mouse click -> Walk\nRight mouse click -> Interact",
        "Got it?"
    };
    
    private readonly string[] inventory = {
        "Company items are valuable!",
        "That's why CosmosX has equipped\n you with state-of-the-art inventory.",
        "Which can be found on the left bottom corner."
    };
    
    private readonly string[] handItem = {
        "Some items are carried by hand.",
        "You can view your hand item at the top left corner.",
    };
    
    private readonly string[] exchange = {
        "Some items can hide other items in them.",
        "hide an item from your inventory by clicking in it."
    };
    
    
    
    // Start is called before the first frame update
    void Start()
    {
        if (AudioManager.numDay > 1)
        {
            Destroy(this);
        }
        else
        {
            GameManager.Instance.inventory.onFirstTimeUse += IntroduceInventory;
            GameManager.Instance.inventory.onFirstTimeHandItemUse += IntroduceHandObject;
            GameManager.Instance.inventory.GetInventoryUI().onFirstExchange += IntroduceExchange;
            StartCoroutine(IntroduceControls());
        }
    }
    
    private IEnumerator IntroduceControls()
    {
        yield return new WaitForSeconds(7f);
        GameManager.Instance.SpeechManager.StartSpeech(GameManager.Instance.PlayerTransform.position, intro);
    }
    
    private void IntroduceInventory()
    {
        GameManager.Instance.inventory.onFirstTimeUse -= IntroduceInventory;
        GameManager.Instance.SpeechManager.StartSpeech(GameManager.Instance.PlayerTransform.position, inventory);

    }
    
    private void IntroduceHandObject()
    {
        GameManager.Instance.inventory.onFirstTimeHandItemUse -= IntroduceHandObject;
        GameManager.Instance.SpeechManager.StartSpeech(GameManager.Instance.PlayerTransform.position, handItem);
    }
    
    private void IntroduceExchange()
    {
        GameManager.Instance.inventory.GetInventoryUI().onFirstExchange -= IntroduceExchange;
        GameManager.Instance.SpeechManager.StartSpeech(GameManager.Instance.PlayerTransform.position, exchange);
    }
}