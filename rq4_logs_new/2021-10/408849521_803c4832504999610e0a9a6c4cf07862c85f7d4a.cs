using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CharacterScript : MonoBehaviour,IHealth
{
    enum Character_states { Idle, Move_to_Target, Attack, Death}
    int DPS = 10;
    float attack_time_interval = 0.5f;
    float attack_timer;
    Character_states my_state = Character_states.Idle;

    private int MHP = 1000, CHP = 1000, _level = 0;
    Building current_target;

    Vector3 velocity;
    private float character_speed = 3f;

    // Start is called before the first frame update
    void Start()
    {
    
    }

    // Update is called once per frame
    void Update()
    {
        switch (my_state)
        {

            case Character_states.Idle:


                break;

            case Character_states.Move_to_Target:


                if (within_melee_range(current_target))
                {
                    my_state = Character_states.Attack;
                    attack_timer = 0;
                    velocity = Vector3.zero;
                }

                transform.position += velocity * Time.deltaTime;
                break;

            case Character_states.Attack:

                if (attack_timer <= 0f)
                {
                    current_target.takeDamage((int)((float)DPS * attack_time_interval));
                    attack_timer = attack_time_interval;
                }

                attack_timer -= Time.deltaTime;

                break;


            case Character_states.Death:


                break;



        }





        if (Input.GetKeyDown(KeyCode.Space))
        {
            current_target = FindObjectOfType<Building>();
            if (current_target)
            {
                assign_target(current_target);

            }
        }









            if (Input.GetKey(KeyCode.DownArrow)) velocity = Vector3.back;
            if (Input.GetKey(KeyCode.LeftArrow)) velocity = Vector3.left;
            if (Input.GetKey(KeyCode.RightArrow)) velocity = Vector3.right;
            if (Input.GetKey(KeyCode.UpArrow)) velocity = Vector3.forward;
        
    }

    public void assign_target(Building current_target)
    {
        if ((my_state == Character_states.Idle)  || (my_state == Character_states.Move_to_Target))
        {
            Vector3 from_me_to_building = current_target.transform.position - transform.position;
            Vector3 direction = from_me_to_building.normalized;
            velocity = direction * character_speed;
            my_state = Character_states.Move_to_Target;
        }
    }

    private bool within_melee_range(Building current_target)
    {
        return (Vector3.Distance(transform.position, current_target.transform.position) < current_target.Melee_distance);
    }

    public void repair(int v)
    {
        throw new System.NotImplementedException();
    }

    public void takeDamage(int v)
    {
        throw new System.NotImplementedException();
    }
}