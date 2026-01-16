using Profile;
using Tools;
using UnityEngine;

public class FightWindowController : BaseController
{
    private readonly ResourcePath _viewPath = new ResourcePath {PathResource = "Prefabs/Fight Window"};
    private FightWindowView _fightWindowViewInstance;
    private ProfilePlayer _profilePlayer;
  
    private int _allCountMoneyPlayer;
    private int _allCountHealthPlayer;
    private int _allCountForcePlayer;
  
    private Money _money;
    private Health _heath;
    private Power _force;
 
    private Enemy _enemy;
    private CrimeLevel _crimeLevel;
  
    public FightWindowController(Transform placeForUi, ProfilePlayer profilePlayer)
    {
        _fightWindowViewInstance = LoadView(placeForUi);
        RefreshView();
        AddGameObjects(_fightWindowViewInstance.gameObject);
        _profilePlayer = profilePlayer;
    }
  
    public void RefreshView()
    {
        _enemy = new Enemy("Enemy Flappy");
    
        _money = new Money(nameof(Money));
        _money.Attach(_enemy);
        _heath = new Health(nameof(Health));
        _heath.Attach(_enemy);
        _force = new Power(nameof(Power));
        _force.Attach(_enemy);
        
        _crimeLevel = new CrimeLevel(nameof(CrimeLevel));
        _crimeLevel.Attach(_enemy);
     
        SubscribeButtons();
    }
  
    private void SubscribeButtons()
    {
        _fightWindowViewInstance.AddCoinsButton.onClick.AddListener(() => ChangeMoney(true));
        _fightWindowViewInstance.MinusCoinsButton.onClick.AddListener(() => ChangeMoney(false));
    
        _fightWindowViewInstance.AddHealthButton.onClick.AddListener(() => ChangeHealth(true));
        _fightWindowViewInstance.MinusHealthButton.onClick.AddListener(() => ChangeHealth(false));
    
        _fightWindowViewInstance.AddPowerButton.onClick.AddListener(() => ChangeForce(true));
        _fightWindowViewInstance.MinusPowerButton.onClick.AddListener(() => ChangeForce(false));
    
        _fightWindowViewInstance.FightButton.onClick.AddListener(Fight);
        _fightWindowViewInstance.LeaveFightButton.onClick.AddListener(CloseWindow);
    
        _fightWindowViewInstance.EscapeButton.onClick.AddListener(Escape);
        _fightWindowViewInstance.CrimeLevelButton.onClick.AddListener(ChangeCrimeLevel);
    }
  
    private void ChangeMoney(bool isAddCount)
    {
        if (isAddCount)
            _allCountMoneyPlayer++;
        else
            _allCountMoneyPlayer--;

        ChangeDataWindow(_allCountMoneyPlayer, DataType.Money);
    }
  
    private void ChangeHealth(bool isAddCount)
    {
        if (isAddCount)
            _allCountHealthPlayer++;
        else
            _allCountHealthPlayer--;

        ChangeDataWindow(_allCountHealthPlayer, DataType.Health);
    }
  
    private void ChangeForce(bool isAddCount)
    {
        if (isAddCount)
            _allCountForcePlayer++;
        else
            _allCountForcePlayer--;

        ChangeDataWindow(_allCountForcePlayer, DataType.Power);
    }

    private void Fight()
    {
        Debug.Log(_allCountForcePlayer >= _enemy.Power
            ? "<color=#07FF00>Win!!!</color>"
            : "<color=#FF0000>Lose!!!</color>");
    }

    private void ChangeDataWindow(int countChangeData, DataType dataType)
    {
        switch (dataType)
        {
            case DataType.Money:
                _fightWindowViewInstance.CountMoneyText.text = $"Player Money {countChangeData.ToString()}";
                _money.Money = countChangeData;
                break;
        
            case DataType.Health:
                _fightWindowViewInstance.CountHealthText.text = $"Player Health {countChangeData.ToString()}";
                _heath.Health = countChangeData;
                break;
        
            case DataType.Power:
                _fightWindowViewInstance.CountPowerText.text = $"Player Force {countChangeData.ToString()}";
                _force.Power = countChangeData;
                break;
       
            case DataType.CrimeLevel:
                _fightWindowViewInstance.CrimeLevelText.text = $"Player Crime Level {countChangeData.ToString()}";
                _crimeLevel.CrimeLevel = countChangeData;
                break;
        }
     
        _fightWindowViewInstance.CountPowerEnemyText.text = $"Enemy Force {_enemy.Power}";
    }
  
    private void CloseWindow()
    {
        _profilePlayer.CurrentState.Value = GameState.Start;
    }
  
    protected override void OnDispose()
    {
        _fightWindowViewInstance.AddCoinsButton.onClick.RemoveAllListeners();
        _fightWindowViewInstance.MinusCoinsButton.onClick.RemoveAllListeners();
    
        _fightWindowViewInstance.AddHealthButton.onClick.RemoveAllListeners();
        _fightWindowViewInstance.MinusHealthButton.onClick.RemoveAllListeners();
    
        _fightWindowViewInstance.AddPowerButton.onClick.RemoveAllListeners();
        _fightWindowViewInstance.MinusPowerButton.onClick.RemoveAllListeners();
    
        _fightWindowViewInstance.FightButton.onClick.RemoveAllListeners();
        _fightWindowViewInstance.LeaveFightButton.onClick.RemoveAllListeners();
    
        _fightWindowViewInstance.EscapeButton.onClick.RemoveAllListeners();
        _fightWindowViewInstance.CrimeLevelButton.onClick.RemoveAllListeners();
    
        _money.Detach(_enemy);
        _heath.Detach(_enemy);
        _force.Detach(_enemy);
        
        GameObject.Destroy(_fightWindowViewInstance.gameObject);
     
        base.OnDispose();
    }

    private FightWindowView LoadView(Transform placeForUi)
    {
        GameObject objectView = Object.Instantiate(ResourceLoader.LoadPrefab(_viewPath), placeForUi, false);
        AddGameObjects(objectView);
        return objectView.GetComponent<FightWindowView>();
    }
 
    private void ChangeCrimeLevel()
    {
        int _allCrimeLevelPlayer = Random.Range(0, 6);

        ChangeDataWindow(_allCrimeLevelPlayer, DataType.CrimeLevel);
    }

    private void Escape()
    {
        Debug.Log("Trying to escape...");
        if (_crimeLevel.CrimeLevel < 3)
        {
            Debug.Log("<color=#0700FF>Escaped successfully</color>");
        }
        else
        {
            Debug.Log("<color=#FF0000>You cannot escape, FIGHT!!!</color>");
            Fight();
        }
    }
}