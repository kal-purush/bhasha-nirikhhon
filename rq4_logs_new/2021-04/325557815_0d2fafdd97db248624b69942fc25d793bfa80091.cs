using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class PrayRoomEnding : MonoBehaviour
{
    public GameObject[] m_prayer;
    private Animator m_Anim0;
    private Animator m_Anim1;
    private Animator m_Anim2;
    private Animator m_Anim3;
    private Animator m_Anim4;
    private Animator m_Anim5;
    private Animator m_Anim6;
    private Animator m_Anim7;
    private Animator m_Anim8;
    private Animator m_Anim9;
    private Animator m_Anim10;
    private Animator m_Anim11;
    private Animator m_Anim12;
    private Animator m_Anim13;
    private Animator m_Anim14;
    private Animator m_Anim15;
    private Animator m_Anim16;
    private Animator m_Anim17;
    private Animator m_Anim18;
    private Animator m_Anim19;

    // Start is called before the first frame update
    void Start()
    {
        m_Anim0 = m_prayer[0].GetComponent<Animator>();
        m_Anim1 = m_prayer[1].GetComponent<Animator>();
        m_Anim2 = m_prayer[2].GetComponent<Animator>();
        m_Anim3 = m_prayer[3].GetComponent<Animator>();
        m_Anim4 = m_prayer[4].GetComponent<Animator>();
        m_Anim5 = m_prayer[5].GetComponent<Animator>();
        m_Anim6 = m_prayer[6].GetComponent<Animator>();
        m_Anim7 = m_prayer[7].GetComponent<Animator>();
        m_Anim8 = m_prayer[8].GetComponent<Animator>();
        m_Anim9 = m_prayer[9].GetComponent<Animator>();
        m_Anim10 = m_prayer[10].GetComponent<Animator>();
        m_Anim11 = m_prayer[11].GetComponent<Animator>();
        m_Anim12 = m_prayer[12].GetComponent<Animator>();
        m_Anim13 = m_prayer[13].GetComponent<Animator>();
        m_Anim14 = m_prayer[14].GetComponent<Animator>();
        m_Anim15 = m_prayer[15].GetComponent<Animator>();
        m_Anim16 = m_prayer[16].GetComponent<Animator>();
        m_Anim17 = m_prayer[17].GetComponent<Animator>();
        m_Anim18 = m_prayer[18].GetComponent<Animator>();
        m_Anim19 = m_prayer[19].GetComponent<Animator>();
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    private void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.CompareTag("Player"))
        {
            Debug.Log("player enter");
            bool wearCape = other.gameObject.GetComponent<player>().m_wearCape;
            Debug.Log(wearCape);
            if (!wearCape)
            {
                m_Anim0.SetBool("PrayRoomEnding", true);
                m_Anim1.SetBool("PrayRoomEnding", true);
                m_Anim2.SetBool("PrayRoomEnding", true);
                m_Anim3.SetBool("PrayRoomEnding", true);
                m_Anim4.SetBool("PrayRoomEnding", true);
                m_Anim5.SetBool("PrayRoomEnding", true);
                m_Anim6.SetBool("PrayRoomEnding", true);
                m_Anim7.SetBool("PrayRoomEnding", true);
                m_Anim8.SetBool("PrayRoomEnding", true);
                m_Anim9.SetBool("PrayRoomEnding", true);
                m_Anim10.SetBool("PrayRoomEnding", true);
                m_Anim11.SetBool("PrayRoomEnding", true);
                m_Anim12.SetBool("PrayRoomEnding", true);
                m_Anim13.SetBool("PrayRoomEnding", true);
                m_Anim14.SetBool("PrayRoomEnding", true);
                m_Anim15.SetBool("PrayRoomEnding", true);
                m_Anim16.SetBool("PrayRoomEnding", true);
                m_Anim17.SetBool("PrayRoomEnding", true);
                m_Anim18.SetBool("PrayRoomEnding", true);
                m_Anim19.SetBool("PrayRoomEnding", true);
                StartCoroutine(gameover());
            }
        }
    }

    IEnumerator gameover()
    {
        WaitForSeconds Delay2sec = new WaitForSeconds(10f);
        yield return Delay2sec;

        SceneManager.LoadScene("GameOver");
    }
}