using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AttackController
{
    private AttackAction attack;

    public AttackController(AttackAction _attack)
    {
        attack = _attack;
    }

    private double DidHit(double _myAimStat, double _myTN, double _targetAgility, double _targetTN, double _typeRateBonus)
    {
        // Calculate hit chance
        double bonus = attack.owner.rateBonuses["accuracy"] + _typeRateBonus;
        bonus = bonus.EnforceRange(0.9, -0.9);
        double totalAccuracy = attack.baseAccuracy + (attack.baseAccuracy * bonus);

        double hitChance = Math.Ceiling(totalAccuracy + (_myAimStat * _myTN) - (_targetAgility * _targetTN));
        hitChance = hitChance.EnforceRange(99, 0);
        
        // Roll two numbers against hit chance
        int rollA = UnityEngine.Random.Range(1, 101);
        int rollB = UnityEngine.Random.Range(1, 101);
        // Return damage multiplier based on the two hit rolls
        if (hitChance >= rollA && hitChance >= rollB)
        {
            return 1.1;
        }
        else if (hitChance >= rollA || hitChance >= rollB)
        {
            return 1.0;
        }
        else
        {
            return 0;
        }

    }

    private double DidCrit(double _rateBonus, double _damageBonus)
    {
        // Roll a single random number against the crit chance
        int roll = UnityEngine.Random.Range(1, 101);

        // Multiply the damage by 1.5 if the attack scored a critical hit
        double totalCritChance = attack.critChance + (attack.critChance * _rateBonus);
        totalCritChance = totalCritChance.EnforceRange(99, 0);
        if (totalCritChance >= roll)
        {
            return 1.5 + _damageBonus;
        }
        else
        {
            return 1;
        }

    }

    private double CalculateDamage(double _hit, double _crit, double _myAttStat, double _myLevel, double _targetDef)
    {
        // Calculate the total damage dealt by the attack
        double damage = _hit * _crit * Math.Sqrt(_myAttStat / _targetDef) * attack.baseDamage * (_myLevel / 2);
        double bonuses;
        // Apply the relevant modifiers
        if (attack.attackType == "physical") {
            bonuses = attack.owner.damageBonuses["damage"] + attack.owner.damageBonuses["physical"];
        }
        else {
            bonuses = attack.owner.damageBonuses["damage"] + attack.owner.damageBonuses["magical"] + attack.owner.damageBonuses[attack.attackType];
        }
        damage = damage + (damage * bonuses);
        return damage;
    }

    public DamageDealt DealDamage(Unit _target)
    {
        // Identify the correct attack stat
        double myAttackStat;
        double myAimStat;
        double typeRateBonus;
        if (attack.attackType == "physical") 
        { 
            myAttackStat = attack.owner.strength;
            myAimStat = attack.owner.dexterity;
            typeRateBonus = attack.owner.rateBonuses["physical"];
        }
        else
        {
            myAttackStat = attack.owner.willpower;
            myAimStat = attack.owner.focus;
            typeRateBonus = attack.owner.rateBonuses["magical"];
        }

        double hit = DidHit(myAimStat, attack.owner.TN_current, _target.agility, _target.TN_current, typeRateBonus);
        double crit = DidCrit(attack.owner.rateBonuses["critical"], attack.owner.damageBonuses["critical"]);

        DamageDealt damageData = new DamageDealt();
        damageData.damage = CalculateDamage(hit, crit, myAttackStat, attack.owner.level, _target.endurance);
        damageData.crit = crit;
        damageData.aggro = attack.baseAggro;
        damageData.type = attack.attackType;

        return damageData;

    }
}