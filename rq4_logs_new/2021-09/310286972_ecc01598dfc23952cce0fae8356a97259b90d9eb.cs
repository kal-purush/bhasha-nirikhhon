using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class St3TreeSound : MonoBehaviour
{
    [SerializeField] private GameObject[] trees;
    [SerializeField] private CriAtomSource[] cris;
    private GameObject player;

    // Start is called before the first frame update
    void Start()
    {
        player = GameObject.Find("Player");
        trees = new GameObject[transform.childCount];
        cris = new CriAtomSource[transform.childCount];
        StartCoroutine(StartSettings());
    }

    private IEnumerator StartSettings()
    {
        yield return StartCoroutine(GetTrees());
        yield return StartCoroutine(GetCris());
        StartCoroutine(CheckDistances());
    }

    private IEnumerator GetTrees()
    {
        int count = 0;
        foreach (Transform tree in transform)
        {
            trees[count] = tree.gameObject;
            count++;
            if (count % 100 == 0) yield return null;
        }
    }

    private IEnumerator GetCris()
    {
        int count = 0;
        foreach (var tree in trees)
        {
            var cri = tree.GetComponent<CriAtomSource>();
            if (cri != null) cris[count] = cri;
            else cris[count] = GetComponentInChildren<CriAtomSource>();

            count++;
            if (count % 30 == 0) yield return null;
        }
    }

    private IEnumerator CheckDistances()
    {
        Debug.Log("Tree Sound Loop Start!");
        for (int i = 0; i < trees.Length; i++)
        {
            if (trees[i] != null)
            {
                float distance = Vector3.Distance(trees[i].transform.position, player.transform.position);
                if (distance <= 20f)
                {
                    PlayAndStopSound(cris[i]);
                }
                else cris[i].Stop();
            }
            if (i % 30 == 0) yield return null;
        }
        Debug.Log("Tree Sound Loop Finish!");

        RestartLoop();
    }

    //再生状況監視
    private void PlayAndStopSound(CriAtomSource cri)
    {
        if (cri != null)
        {
            if ((cri.status == CriAtomSource.Status.Stop) || (cri.status == CriAtomSource.Status.PlayEnd))
            {
                cri.Play();
            }
        }
    }

    private void RestartLoop()
    {
        StartCoroutine(CheckDistances());
    }
}