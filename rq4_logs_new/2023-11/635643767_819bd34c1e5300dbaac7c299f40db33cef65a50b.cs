using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public enum Mode
{
    Enable,
    Disable
}
public enum Ability
{
    Basic,
    Ranged,
    Bomb,
    Dash,
}
public class AbilityEnabler : MonoBehaviour
{
    [SerializeField] private Mode mode;
    [SerializeField] private Ability ability;

    private GameObject player;
    private bool activated = false;


    // Start is called before the first frame update
    void Start()
    {
        player = GameObject.Find("Player");
    }

    // Update is called once per frame
    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.CompareTag("Player") && !activated)
        {
            Configure();
        }
    }
    private void Configure()
    {
        switch (ability)
        {
            case Ability.Basic:
                if(mode == Mode.Enable)
                {
                    player.GetComponentInChildren<BasicAbility>().enabled = true;
                }
                else
                {
                    player.GetComponentInChildren<BasicAbility>().enabled = false;
                }
                break;
            case Ability.Ranged:
                if (mode == Mode.Enable)
                {
                    player.GetComponentInChildren<RangeAbility>().enabled = true;
                }
                else
                {
                    player.GetComponentInChildren<RangeAbility>().enabled = false;
                }
                break;
            case Ability.Bomb:
                if (mode == Mode.Enable)
                {
                    player.GetComponentInChildren<BombAbility>().enabled = true;
                }
                else
                {
                    player.GetComponentInChildren<BombAbility>().enabled = false;
                }
                break;
            case Ability.Dash:
                if (mode == Mode.Enable)
                {
                    player.GetComponent<PlayerMovement>().dashEnabled = true;
                }
                else
                {
                    player.GetComponent<PlayerMovement>().dashEnabled = false;
                }
                break;
        }
        activated = true;
    }
}