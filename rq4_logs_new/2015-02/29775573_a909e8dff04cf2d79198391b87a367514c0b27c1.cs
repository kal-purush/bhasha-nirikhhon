using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class ChurrosSeller : MonoBehaviour {

    // Satisfaction and disposition for this event
    public float satisfacao;
    public float disposicao;

    public string text;
    
    // Action Text
    public Text actionText;
    public Text actionText_shadow;
    
    // ChurrosSeller sprite and dialogs
    public GameObject[] churrosSprites;
    public GameObject[] dialogsChurros;

    // player
    public GameObject player;
    
    Transform playerTransform;
    Animator playerAnimator;

    GameController gameController;
    bool churrosActive = true;

	// Use this for initialization
	void Start () {
        // finds the GameController
        var find_gameController = GameObject.Find("GameController");
        if (find_gameController == null)
            print("GameController not found");
        else
            gameController = find_gameController.GetComponent<GameController>();

        playerTransform = player.GetComponent<Transform>();
        playerAnimator = player.GetComponent<Animator>();
    }

    IEnumerator Churros0()
    {
        churrosSprites[0].gameObject.renderer.enabled = false;
        churrosSprites[1].gameObject.renderer.enabled = true;
        actionText.text = actionText_shadow.text = "";
        playerAnimator.SetBool("blocked", true);

        if (gameController != null)
        {
            gameController.diminuiDisposicao(disposicao);
            print("disposicao = " + gameController.getDisposicao());
        }
        
        Instantiate(dialogsChurros[0]);

        yield return new WaitForSeconds(3);

        Destroy(GameObject.FindGameObjectWithTag("dialog"));

        // Random for reaction
        var val = Random.Range(0, 10);
        if (val < 6)
            StartCoroutine(Churros1());
        else
            StartCoroutine(Churros2());
    }

    IEnumerator Churros1()
    {   //positive
        if(gameController != null)
        {
            gameController.aumentaDisposicao(disposicao);
            print("disposicao = " + gameController.getDisposicao());

            gameController.aumentaSatisfacao(satisfacao);
            print("satisfacao = " + gameController.getSatisfacao());
        }
        
        Instantiate(dialogsChurros[1]);

        yield return new WaitForSeconds(3);

        Destroy(GameObject.FindGameObjectWithTag("dialog"));

        ChurrosFim();
    }

    IEnumerator Churros2()
    {   //negative
        if(gameController != null)
        {
            gameController.diminuiDisposicao(disposicao);
            print("disposicao = " + gameController.getDisposicao());
            gameController.diminuiSatisfacao(satisfacao);
            print("satisfacao = " + gameController.getSatisfacao());
        }
        
        Instantiate(dialogsChurros[2]);

        yield return new WaitForSeconds(3);

        Destroy(GameObject.FindGameObjectWithTag("dialog"));

        
        Instantiate(dialogsChurros[3]);

        yield return new WaitForSeconds(3);

        Destroy(GameObject.FindGameObjectWithTag("dialog"));

        // Random for reaction
        var val = Random.Range(0, 10);
        if (val > 4)
        {   //positive
            if(gameController != null)
            {
                gameController.aumentaDisposicao(disposicao);
                print("disposicao = " + gameController.getDisposicao());

                gameController.aumentaSatisfacao(satisfacao);
                print("satisfacao = " + gameController.getSatisfacao());
            }
            
            Instantiate(dialogsChurros[4]);

            yield return new WaitForSeconds(5);

            Destroy(GameObject.FindGameObjectWithTag("dialog"));

            Instantiate(dialogsChurros[5]);

            yield return new WaitForSeconds(2);

            Destroy(GameObject.FindGameObjectWithTag("dialog"));
        }

        else
        {
            if(gameController != null)
            {
                gameController.diminuiSatisfacao(satisfacao);
                print("satisfacao = " + gameController.getSatisfacao());
            }

            Instantiate(dialogsChurros[6]);

            yield return new WaitForSeconds(3);

            Destroy(GameObject.FindGameObjectWithTag("dialog"));
        }

        ChurrosFim();
    }

    void ChurrosFim()
    {
        churrosSprites[0].gameObject.renderer.enabled = true;
        churrosSprites[1].gameObject.renderer.enabled = false;
        playerAnimator.SetBool("blocked", false);
    }

    void OnTriggerStay(Collider other)
    {
        if (other.tag == "Player" && playerTransform.localScale.x < 0 && churrosActive == true)
        {
            actionText.text = actionText_shadow.text = text;

            if (Input.GetKey(KeyCode.Space))
            {
                churrosActive = false;
                StartCoroutine(Churros0());
            }
        }
        else
            actionText.text = actionText_shadow.text = "";
    }

    void OnTriggerExit(Collider other)
    {
        if (other.tag == "Player")
            actionText.text = actionText_shadow.text = "";
    }

}