using System;
using System.Collections;
using System.Collections.Generic;
using NUnit.Framework;
using ReupVirtualTwin.behaviours;
using ReupVirtualTwin.helpers;
using ReupVirtualTwin.models;
using UnityEditor;
using UnityEngine;
using UnityEngine.TestTools;

public class ObjectRegistryTest : MonoBehaviour
{
    GameObject baseGlobalScriptsPrefab = AssetDatabase.LoadAssetAtPath<GameObject>("Packages/com.reup.romulo/Assets/Quickstart/BaseGlobalScripts.prefab");
    GameObject characterPrefab = AssetDatabase.LoadAssetAtPath<GameObject>("Packages/com.reup.romulo/Assets/Quickstart/Character.prefab");
    GameObject character;
    GameObject baseGlobalScripts;
    GameObject testObj;
    ObjectRegistry objectRegistry;

    [SetUp]
    public void SetUp()
    {
        baseGlobalScripts = (GameObject)PrefabUtility.InstantiatePrefab(baseGlobalScriptsPrefab);
        character = (GameObject)PrefabUtility.InstantiatePrefab(characterPrefab);
        testObj = new GameObject("testObj");
        testObj.AddComponent<RegisteredIdentifier>();
        objectRegistry = ObjectFinder.FindObjectRegistry().GetComponent<ObjectRegistry>();
    }

    [TearDown]
    public void TearDown()
    {
        Destroy(baseGlobalScripts);
        Destroy(testObj);
        Destroy(character);
    }

    [UnityTest]
    public IEnumerator TestObjHasAnId()
    {
        var id = testObj.GetComponent<RegisteredIdentifier>().getId();
        Assert.IsNotNull(id);
        yield return null;
    }

    [UnityTest]
    public IEnumerator ObjectRegistryContainsTestObj()
    {
        var id = testObj.GetComponent<RegisteredIdentifier>().getId();
        var obtainedObj = objectRegistry.GetObjectWithGuid(id);
        Assert.AreEqual(obtainedObj, testObj);
        yield return null;
    }

    [UnityTest]
    public IEnumerator NoObjectIsReturnedIfIncorrectId()
    {
        var obtainedObj = objectRegistry.GetObjectWithGuid("an-incorrect-id");
        Assert.AreEqual(obtainedObj, null);
        yield return null;
    }
}