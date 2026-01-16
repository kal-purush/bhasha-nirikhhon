using System;
using SplashKitSDK;

public class Attack : Move
{
    private int _damageAmount;
    private bool _counterSuccess;
    private bool _attackHit;
    private double _accuracy;

    public Attack(Character Caster, Character Target, int power, double accuracy)
    {
        _Caster = Caster;
        _Target = Target;
        _accuracy = accuracy;

        _damageAmount = _Caster.Level * (10 + SplashKit.Rnd(power));
        _counterSuccess = false;
        _attackHit = false;
    }

    public override void Execute()
    {
        if(_Target.Countered == true && SplashKit.Rnd() < 0.5)
        {
            _Caster.TakeDamage(_damageAmount/2);
            _counterSuccess = true;
        }
        else
        {
            if(SplashKit.Rnd() < _accuracy)
            {
                _Target.TakeDamage(_damageAmount);
                _attackHit = true;
            }
        }

    }

    public override string Message()
    {
        if(_counterSuccess != true)
        {
            if(_attackHit == true)
            {
                return $"{_Caster.Name} attacks {_Target.Name} for {_damageAmount} damage";
            }
            else
            {
                return $"{_Caster.Name} attacked {_Target.Name} but it missed";
            }
        }
        else
        {
            return $"{_Target.Name} countered the foe's attack for {_damageAmount/2} damage";
        }
    }

}