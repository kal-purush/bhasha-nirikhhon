using System.Threading;
using DG.Tweening;
using UnityEngine;
using UnityEngine.SceneManagement;

public class GameEvents : MonoBehaviour
{
    static GameObject bullet;
    static GameObject laser;
    static GameObject borders;
    static GameObject borderLeft;
    static GameObject borderRight;
    static GameObject borderTop;
    static GameObject borderBottom;
    static GameObject player;
    static PlayerCollision playerCollision;

    public static void Init(string level)
    {
        bullet = (GameObject)Resources.Load("Bullet");
        laser = (GameObject)Resources.Load("Laser");
        borders = GameObject.Find("Borders");
        borderLeft = GameObject.Find("Left");
        borderRight = GameObject.Find("Right");
        borderTop = GameObject.Find("Top");
        borderBottom = GameObject.Find("Bottom");
        player = GameObject.Find("Player");
        playerCollision = player.GetComponent<PlayerCollision>();
        UnloadAllScenesExcept(level);
        PlayerPrefs.SetString("level", level);
    }

    public static float GetLeftX()
    {
        return borderLeft.transform.position.x;
    }

    public static float GetMiddleX()
    {
        return borderTop.transform.position.x;
    }

    public static float GetRightX()
    {
        return borderRight.transform.position.x;
    }

    public static float GetTopY()
    {
        return borderTop.transform.position.y;
    }

    public static float GetMiddleY()
    {
        return borderLeft.transform.position.y;
    }

    public static float GetBottomY()
    {
        return borderBottom.transform.position.y;
    }

    public static void EnableDeadlyBorders()
    {
        Color borderColor = new(1.0f, 0.0f, 0.0f);

        borders.tag = "Danger";
        borderLeft.GetComponent<SpriteRenderer>().color = borderColor;
        borderRight.GetComponent<SpriteRenderer>().color = borderColor;
        borderTop.GetComponent<SpriteRenderer>().color = borderColor;
        borderBottom.GetComponent<SpriteRenderer>().color = borderColor;
    }

    public static void DisableDeadlyBorders()
    {
        Color borderColor = new(1.0f, 1.0f, 1.0f);

        borders.tag = "Untagged";
        borderLeft.GetComponent<SpriteRenderer>().color = borderColor;
        borderRight.GetComponent<SpriteRenderer>().color = borderColor;
        borderTop.GetComponent<SpriteRenderer>().color = borderColor;
        borderBottom.GetComponent<SpriteRenderer>().color = borderColor;
    }

    public static GameObject CreateLaser(float posX, float posY, float laserWidth, float laserHeight, float angle)
    {
        GameObject laserClone = Instantiate(laser, new Vector3(posX, posY, 1.0f), Quaternion.Euler(0.0f, 0.0f, 90.0f - angle));
        Laser cloneScript = laserClone.GetComponent<Laser>();

        laserClone.transform.localScale = new Vector3(laserWidth, laserHeight, 1.0f);
        cloneScript.enabled = true;

        return laserClone;
    }

    public static void RotateLaser(GameObject laser, float angle, float duration)
    {
        laser.transform.DORotate(new Vector3(0.0f, 0.0f, 90.0f - laser.transform.rotation.z + angle), duration);
    }

    public static void MoveLaser(GameObject laser, float posX, float posY, float duration)
    {
        laser.transform.DOMove(new Vector3(posX, posY, 1.0f), duration);
    }

    public static GameObject CreateBullet(float posX, float posY, float size, float angle, float speed, int level)
    {
        GameObject bulletClone = Instantiate(bullet, new Vector3(posX, posY, 1.0f), Quaternion.Euler(0.0f, 0.0f, 90.0f - angle));
        Bullet cloneScript = bulletClone.GetComponent<Bullet>();

        bulletClone.transform.localScale *= size;
        cloneScript.enabled = true;
        cloneScript.angle = angle;
        cloneScript.speed = speed;
        cloneScript.level = level;

        return bulletClone;
    }

    public static void BorderLeftScale(float scaleValue, float animationTime)
    {
        borderLeft.transform.DOMoveX(borderLeft.transform.position.x - scaleValue, animationTime);
    }

    public static void BorderRightScale(float scaleValue, float animationTime)
    {
        borderRight.transform.DOMoveX(borderRight.transform.position.x + scaleValue, animationTime);
    }

    public static void BorderTopScale(float scaleValue, float animationTime)
    {
        borderTop.transform.DOMoveY(borderTop.transform.position.y + scaleValue, animationTime);
    }

    public static void BorderBottomScale(float scaleValue, float animationTime)
    {
        borderBottom.transform.DOMoveY(borderBottom.transform.position.y - scaleValue, animationTime);
    }

    public static void PlayerAttraction(float angle, float force, float duration)
    {
        angle = Mathf.PI * angle / 180.0f;
        playerCollision.attractMovement = force * new Vector3(Mathf.Cos(angle), Mathf.Sin(angle), 1.0f);
        playerCollision.isAttracted = true;
        Thread.Sleep((int)duration * 1000);
        playerCollision.isAttracted = false;
    }

    public static void PlayerScale(float scaleValue, float animationTime)
    {
        player.transform.DOScale(new Vector3(player.transform.localScale.x + scaleValue, player.transform.localScale.y + scaleValue, 1.0f), animationTime);
    }

    public static void UnloadAllScenesExcept(string sceneName)
    {
        int sceneNumber = SceneManager.sceneCount;
        for (int i = 0; i < sceneNumber; i++)
        {
            Scene scene = SceneManager.GetSceneAt(i);
            if (scene.name != sceneName)
            {
                SceneManager.UnloadSceneAsync(scene);
            }
        }
    }

    public static void GameWon()
    {
        DOTween.KillAll();
        SceneManager.LoadScene("WinMenu");
    }
}