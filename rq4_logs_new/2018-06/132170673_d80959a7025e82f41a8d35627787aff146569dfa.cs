using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ItemSpawn : MonoBehaviour
{
    /* Tag definida para o jogador */
    private const string PLAYER_TAG = "Player";

    /* Máximo de jogadores por ronda */
    private const int TOTAL_PLAYER_COUNT = 2;
    
    /* Distância máximo/minima da posição inicial a que o spawn pode flutuar */
    private const float MAX_FLOAT_DISTANCE = .5f;

    /* Velocidade do movimento flutuante */
    private const float FLOAT_SPEED = .4f;

    /* Identificador do item, este atributo é alterado no inspector do unity */
    public int itemId;

    /* Item a distribuir */
    private Item item;

    /* Lista de players que já receberam o item */
    private List<Player> pickers;

    private SpriteRenderer renderer;
    private Vector3 startPosition;

    /* Direção do movimento flutuante (-1 ou 1) */
    private float direction;

    void Start()
    {
        renderer = GetComponent<SpriteRenderer>();
        pickers = new List<Player>();

        startPosition = new Vector3(transform.position.x, transform.position.y, 0);
        direction = 1;

        switch (itemId)
        {
            case 0:
                item = new EnergyPotion();
                break;
            case 1:
                item = new LightningBolt();
                break;
            case 2:
                item = new Beef();
                break;
            case 3:
                item = new Star();
                break;
        }
    }

    void Update()
    {
        transform.Translate(new Vector3(0, direction * Time.deltaTime * FLOAT_SPEED, 0));

        /* Quando passa dos limites máximos / minimos, reverter direção */
        if (offLimits())
        {
            direction = -direction;
        }
    }

    private bool offLimits()
    {
        return transform.position.y > (startPosition.y + MAX_FLOAT_DISTANCE) || transform.position.y < (startPosition.y - MAX_FLOAT_DISTANCE);
    }

    public void OnTriggerEnter2D(Collider2D other)
    {
        if (other.CompareTag(PLAYER_TAG))
        {
            Player player = other.gameObject.GetComponent<Player>();

            if (!pickers.Contains(player))
            {
                player.inventory.add(item);
                pickers.Add(player);

                /* Se todos os players já receberam o item, destruir o spawn */
                if (pickers.Count == TOTAL_PLAYER_COUNT)
                {
                    Destroy(gameObject);
                }
                /* Se o jogador local o apanhou, reduzir a opacidade para 30% */
                else if (player.user.name.Equals(GameManager.instance.getLocalUser().name))
                {
                    renderer.color = new Color(1f, 1f, 1f, .3f);
                }
            }
        }
    }
}