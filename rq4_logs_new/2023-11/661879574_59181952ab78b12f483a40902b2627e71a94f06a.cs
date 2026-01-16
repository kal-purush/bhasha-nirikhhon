using NUnit.Framework;
using UnityEngine;
using UnityEngine.TestTools;
using UnityEditor;
using System.Collections;
using ReupVirtualTwin.behaviours;
using ReupVirtualTwin.characterMovement;

public class CollisionDetectorTest : MonoBehaviour
{
    GameObject characterPrefab = AssetDatabase.LoadAssetAtPath<GameObject>("Packages/com.reup.romulo/Assets/Quickstart/Character.prefab");
    GameObject cubePrefab = AssetDatabase.LoadAssetAtPath<GameObject>("Packages/com.reup.romulo/Tests/TestAssets/Cube.prefab");
    GameObject character;
    GameObject widePlatform;
    GameObject wall;
    CharacterPositionManager posManager;

    [SetUp]
    public void SetUp()
    {
        character = (GameObject)PrefabUtility.InstantiatePrefab(characterPrefab);
        posManager = character.GetComponent<CharacterPositionManager>();
        widePlatform = (GameObject)PrefabUtility.InstantiatePrefab(cubePrefab);
        SetPlatform();
    }

    [TearDown]
    public void TearDown()
    {
        Destroy(character);
        Destroy(widePlatform);
    }

    [UnityTest]
    public IEnumerator CharacterShouldNotCrossWall()
    {
        wall = (GameObject)PrefabUtility.InstantiatePrefab(cubePrefab);
        SetWallAt2MetersInZAxis();

        character.transform.position = new Vector3(0, 1.5f, 0);
        yield return new WaitForSeconds(0.2f);

        posManager.WalkToTarget(new Vector3(0, 0, 5));

        yield return new WaitForSeconds(2f);

        Assert.LessOrEqual(character.transform.position.z, 2);

        Destroy(wall);
    }
    private void SetPlatform()
    {
        widePlatform.transform.localScale = new Vector3(10, 0.1f, 10);
        widePlatform.transform.position = new Vector3(0, -0.05f, 0);
    }
    private void SetWallAt2MetersInZAxis()
    {
        wall.transform.localScale = new Vector3(10, 10, 0.1f);
        wall.transform.position = new Vector3(0, 0, 2.05f);
    }
}