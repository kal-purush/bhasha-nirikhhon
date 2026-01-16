using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Cember : MonoBehaviour
{
    public GameObject _AitOlduguStand;
    public GameObject _AitOlduguCemberSoketi;
    public bool HareketEdebilirmi;
    public string Renk;
    public GameManager _GameManager;
    GameObject HareketPozisyonu, GidecegiStand;
    bool Secildi, PosDegistir, SoketOtur, SoketeGeriGit;

    public void HareketEt(string islem, GameObject Stand = null, GameObject Soket = null, GameObject GidilecekObje = null)
    {
        switch (islem)
        {
            case "Secim":
                HareketPozisyonu = GidilecekObje;
                Secildi = true;
                break;
            case "PozisyonDegistir":
                GidecegiStand = Stand;
                _AitOlduguCemberSoketi = Soket;
                HareketPozisyonu = GidilecekObje;
                PosDegistir = true;
                break;
            case "SoketeGeriGit":
                SoketeGeriGit = true;
                break;

        }
    }
    // Update is called once per frame
    void Update()
    {
        if (Secildi)
        {
            transform.position = Vector3.Lerp(transform.position, HareketPozisyonu.transform.position, .2f);
            if (Vector3.Distance(transform.position, HareketPozisyonu.transform.position) < .10)
            {
                Secildi = false;
            }
        }
        if (PosDegistir)
        {
            transform.position = Vector3.Lerp(transform.position, HareketPozisyonu.transform.position, .2f);
            if (Vector3.Distance(transform.position, HareketPozisyonu.transform.position) < .10)
            {
                PosDegistir = false;
                SoketOtur = true;
            }
        }
        if (SoketOtur)
        {
            transform.position = Vector3.Lerp(transform.position, _AitOlduguCemberSoketi.transform.position, .2f);
            if (Vector3.Distance(transform.position, _AitOlduguCemberSoketi.transform.position) < .10)
            {
                transform.position = _AitOlduguCemberSoketi.transform.position;
                SoketOtur = false;
                _AitOlduguStand = GidecegiStand;
                if (_AitOlduguStand.GetComponent<Stand>()._Cemberler.Count > 1)
                {
                    _AitOlduguStand.GetComponent<Stand>()._Cemberler[_AitOlduguStand.GetComponent<Stand>()._Cemberler.Count-2].GetComponent<Cember>().HareketEdebilirmi = false;
                }
                _GameManager.HareketVar = false;
            }
        }
        if (SoketeGeriGit)
        {
            transform.position = Vector3.Lerp(transform.position, _AitOlduguCemberSoketi.transform.position, .2f);
            if (Vector3.Distance(transform.position, _AitOlduguCemberSoketi.transform.position) < .10)
            {
                transform.position = _AitOlduguCemberSoketi.transform.position;
                SoketeGeriGit = false;
                _GameManager.HareketVar = false;
            }
        }
    }
}