using UnityEngine;
using System.Collections;
using Assets.Scripts.Combat;
using TMPro;
using UnityEngine.UI;

public class WeaponStatusBar : MonoBehaviour
{
    private WeaponManager _weaponManager;
    private double _lastBlink = -1;
    private readonly double _blinkDelay = 0.5;
    public TMP_Text statusText;
    public Slider statusBar;

    // Use this for initialization
    void Start()
    {
        _weaponManager = PlayerManager.Instance.Player.GetComponent<WeaponManager>();
        PlayerManager.OnPlayerChanged += (player) => { if (player.TryGetComponent(out WeaponManager weaponManager)) { _weaponManager = weaponManager; } };
        statusText.color = Color.white;
        statusBar.gameObject.SetActive(false);
    }

    // Update is called once per frame
    void Update()
    {
        BaseWeapon currentWeapon = _weaponManager.CurrentWeapon();
        if (currentWeapon is Gun gun)
        {
            statusText.text = $"{gun.CurrentAmmo} / {gun.MaxAmmo}";
            if (gun.CurrentAmmo == 0)
            {
                if (Time.fixedTimeAsDouble - _lastBlink >= _blinkDelay)
                {
                    _lastBlink = Time.fixedTimeAsDouble; //TODO: beware when pausing game, should have global time control
                    statusText.color = statusText.color == Color.white ? Color.red : Color.white;
                }
            }
            else
            {
                _lastBlink = -1;
                statusText.color = Color.white;
            }
            if (gun.IsReloading())
            {
                statusBar.gameObject.SetActive(true);
                statusBar.value = (float)gun.GetCurrentReloadPercent();
                statusBar.maxValue = 100;
            }
            else
            {
                statusBar.gameObject.SetActive(false);
            }
        }
        else
        {
            statusText.text = "";
            statusBar.gameObject.SetActive(false);
        }
    }
}