using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class AtlasManager : MonoBehaviour {

    public List<AtlasEntry> unlockedEnemyEntries; // Currently, all enemies will be unlocked by default
    public AtlasEntry displayedEnemyEntry;
    public Transform displayPosition;
    public GameObject enemy;
    public Text enemyName;
    public Text enemyDescription;
    public Text designer;
    public GameObject leftArrow;
    public GameObject rightArrow;

    private bool tallEnemy; // Some enemies are too tall and don't appear on top of the EnemyStand
    private int currentIndex;

    public bool heldDown;
	// Use this for initialization
	void Start () {
        ChangeEnemy(0);
	}

    // Update is called once per frame
    void Update()
    {
        if (Input.GetAxis("Horizontal") > 0 && !heldDown)
        {
            heldDown = true;
            rightArrow.GetComponent<Image>().enabled = true;
            currentIndex++;
            if (currentIndex > unlockedEnemyEntries.Count - 1)
            {
                currentIndex = 0;
            }
            ChangeEnemy(currentIndex);
        }
        else if (Input.GetAxis("Horizontal") < 0 && !heldDown)
        {
            heldDown = true;
            leftArrow.GetComponent<Image>().enabled = true;
            currentIndex--;
            if (currentIndex < 0)
            {
                currentIndex = unlockedEnemyEntries.Count - 1;
            }
            ChangeEnemy(currentIndex);
        }
        if (Input.GetAxis("Horizontal") > 0 && !heldDown)
        {
            heldDown = true;
            rightArrow.GetComponent<Image>().enabled = false;
        }
        if (Input.GetAxis("Horizontal") < 0 && !heldDown)
        {
            heldDown = true;
            leftArrow.GetComponent<Image>().enabled = false;
        }

        if (Input.GetAxis("Horizontal") == 0) {
            heldDown = false;
        }
    }

    void ChangeEnemy(int index)
    {
        // Replaces the visible enemy
        Destroy(enemy);
        displayedEnemyEntry = unlockedEnemyEntries[index];
        enemy = Instantiate(displayedEnemyEntry.enemy);
        DisableBehavior();
        enemy.transform.position = displayPosition.position;
        if (tallEnemy)
        {
            enemy.transform.position = new Vector3(enemy.transform.position.x, enemy.transform.position.y + 3, enemy.transform.position.z);
        }

        // Replaces the text
        enemyName.text = displayedEnemyEntry.enemyName;
        enemyDescription.text = displayedEnemyEntry.description;
        designer.text = "Designed By: " + displayedEnemyEntry.designer;
    }

    void UpdateInfo()
    {
        enemyName.text = displayedEnemyEntry.name;
        enemyDescription.text = displayedEnemyEntry.description;
    }

    void DisableBehavior()
    {
        tallEnemy = false;
        // Disables behavior depending on what enemy is set as. 
        // Could be improved/more flexible if all enemy behaviors inherited from one script.
        switch (enemy.name)
        {
            case ("WingedEnemy(Clone)"):
                enemy.GetComponent<BadGuyBehaviour>().enabled = false;
                break;
            case ("AtlasJumper(Clone)"):
                enemy.GetComponent<BouncyEnemyBehaviour>().enabled = false;
                break;
            case ("ChargeEnemy(Clone)"):
                enemy.GetComponent<ChargeShieldBehavior>().enabled = false;
                break;
            case ("Evil Spirit(Clone)"):
                enemy.GetComponent<EvilSpiritBehaviour>().enabled = false;
                break;
            case ("GhostMan(Clone)"):
                enemy.GetComponent<GhostGuyBehaviour>().enabled = false;
                break;
            case ("Mage(Clone)"):
                enemy.GetComponent<MageBehaviour>().enabled = false;
                break;
            case ("ProjectileEnemy(Clone)"):
                enemy.GetComponent<ProjectileEnemyBehavior>().enabled = false;
                tallEnemy = true;
                break;
            case ("Skely-Man(Clone)"):
                enemy.GetComponent<SkelyManBehaviour>().enabled = false;
                break;
            case ("SlowEnemy(Clone)"):
                enemy.GetComponent<SlowEnemyBehavior>().enabled = false;
                break;
            case ("SpawnerPortal(Clone)"):
                enemy.GetComponent<SpawnerBehavior>().enabled = false;
                break;
            default:
                Debug.Log(enemy.name + " needs to be accounted for in AtlasManager's switch statement!");
                break;
        }
        enemy.GetComponent<Rigidbody2D>().isKinematic = true;
        enemy.GetComponent<EnemyDefeatGeneralPurpose>().enabled = false;
    }
}