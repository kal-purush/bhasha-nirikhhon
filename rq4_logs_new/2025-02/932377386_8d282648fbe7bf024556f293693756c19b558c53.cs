using UnityEngine;
using UnityEngine.SceneManagement; 
using UnityEngine.UI;

public class BallScript : MonoBehaviour
{
    public Rigidbody2D rb2d;
    public float speed = 5f;
    private bool isStarted = false;
    private int vidas = 3;
    public int blocosqtde =0;
    void Start()
    {
        if (rb2d == null)
            rb2d = GetComponent<Rigidbody2D>();
    }

    void Update()
    {
        if (!isStarted && Input.GetKeyDown(KeyCode.Space))
        {
            StartBall();
        }

    }
    
void OnGUI()

{
    // Exibe o nome da fase
    //GUI.Label(new Rect(Screen.width / 2 - 50, 10, 300, 300), "Fase 1");
    // Exibe os pontos (blocos restantes)
    GUI.Label(new Rect(Screen.width / 2 + 50, 10, 300, 300), "Pontos: " + blocosqtde);

    // Exibe as vidas
    GUI.Label(new Rect(Screen.width / 2 + 200, 10, 300, 300), "Vidas: " + vidas);
}







    void StartBall()
    {
        isStarted = true;
        rb2d.velocity = new Vector2(Random.Range(-1f, 1f), 1).normalized * speed;
    }

    void OnCollisionEnter2D(Collision2D coll)
    {
        if (coll.collider.CompareTag("Player"))
        {
            Vector2 vel = rb2d.velocity;
            vel.y = (rb2d.velocity.y) + (coll.collider.attachedRigidbody.velocity.y / 3);
            rb2d.velocity = vel;
        }
        else if (coll.collider.CompareTag("Block"))
        {
            // Destruir o objeto que colidiu com a bola
            Destroy(coll.gameObject);
            Debug.Log("Block");
            blocosqtde++;
            CheckForWin();  
        }
        else if (coll.collider.CompareTag("Penalty"))
        {
            // Destruir o objeto que colidiu com a bola
            vidas -= 1;
            Debug.Log("Vidas: " + vidas);
            if (vidas == 0){ 
                Debug.Log("Acabou");
                SceneManager.LoadScene("End");
            } 
            else{
                isStarted = false;
                transform.position = new Vector3(-0.01f, -1.53f, 0f);
                rb2d.velocity = Vector2.zero; // Reseta a velocidade
                Update();
            }
            
        }
    }
        void CheckForWin()
    {
        // Encontra todos os blocos restantes na cena
        GameObject[] blocks = GameObject.FindGameObjectsWithTag("Block");

        Debug.Log("Blocos restantes: " + blocks.Length);  // Verifique a quantidade de blocos restantes

        // Se não houver mais blocos, exibe a mensagem de vitória
        if (blocks.Length == 1)
        {
            Debug.Log("Win");
            //winText.gameObject.SetActive(true);  // Mostra o texto de vitória
            //winText.text = "Você Ganhou!";  // Define o texto de vitória
            LoadNextScene();
        }
    }
    void LoadNextScene()
    {
        // Pega o nome da cena atual
        string currentSceneName = SceneManager.GetActiveScene().name;

        // Verifica o nome da cena atual e carrega a próxima
        if (currentSceneName == "Fase1")
        {
            SceneManager.LoadScene("Fase2");  // Carrega a cena "teste"
        }
        else if (currentSceneName == "Fase2")
        {
            SceneManager.LoadScene("End");  // Carrega a cena "fim"
        }
        else
        {
            Debug.Log("Você completou todas as fases!");
        }
    
    }
}