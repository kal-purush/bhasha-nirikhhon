using System.Collections.Generic;

abstract class DataPlayer
{
    private string _titleData;
    private int _countMoney;
    private int _countHealth;
    private int _countPower;
    private int _countCrimeLevel;
  
    private List<IEnemy> _enemies = new List<IEnemy>();

    protected DataPlayer(string titleData)
    {
        _titleData = titleData;
    }

    public void Attach(IEnemy enemy)
    {
        _enemies.Add(enemy);
    }

    public void Detach(IEnemy enemy)
    {
        _enemies.Remove(enemy);
    }

    protected void Notify(DataType dataType)
    {
        foreach (var investor in _enemies)
            investor.Update(this, dataType);
    }
  
    public string TitleData => _titleData;
  
    public int Money
    {
        get => _countMoney;
        set
        {
            if (_countMoney != value)
            {
                _countMoney = value;
                Notify(DataType.Money);
            }
        }
    }
  
    public int Health
    {
        get => _countHealth;
        set
        {
            if (_countHealth != value)
            {
                _countHealth = value;
                Notify(DataType.Health);
            }
        }
    }
  
    public int Power
    {
        get => _countPower;
        set
        {
            if (_countPower != value)
            {
                _countPower = value;
                Notify(DataType.Power);
            }
        }
    }
  
    public int CrimeLevel
    {
        get => _countCrimeLevel;
        set
        {
            if (_countCrimeLevel != value)
            {
                _countCrimeLevel = value;
                Notify(DataType.CrimeLevel);
            }
        }
    }
}

class Money : DataPlayer
{
    public Money(string titleData)
        : base(titleData)
    {
    }
}

class Health : DataPlayer
{
    public Health(string titleData)
        : base(titleData)
    {
    }
}

class Power : DataPlayer
{
    public Power(string titleData)
        : base(titleData)
    {
    }
}

class CrimeLevel : DataPlayer
{
    public CrimeLevel(string titleData)
        : base(titleData)
    {
    }
}