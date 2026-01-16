using UnityEngine;
using System.Collections;

public class Skill2_003 : MonoBehaviour
{
    private string activeBy;
    [SerializeField]
    private NetworkIdentity networkIdentity;
    private Position direction;

    public string ActiveBy { get => activeBy; set => activeBy = value; }
    public Position Direction { get => direction; set => direction = value; }

    private void OnCollisionEnter2D(Collision2D collision)
    {
        var niActive = NetworkClient.serverObjects[activeBy];
        NetworkIdentity ni = collision?.gameObject?.GetComponent<NetworkIdentity>();

        // cham nhau
        if (ni.tag == "BulletThrough")
        {
            return;
        }

        // ko phai cham chinh minh


        if (ni.GetId() != activeBy)
        {
            if (ni.Team == niActive.Team)
            {
                return;
            }

            // client trung dan gui request
            if (ni.IsControlling())
            {
                gameObject.SetActive(false);
                networkIdentity.GetSocket().Emit("touchSkill", new JSONObject(JsonUtility.ToJson(new TouchData()
                {
                    id = networkIdentity.GetId(),
                    num = 2,
                    typeId = "003",
                    enemyId = ni.GetId(),
                    typeEnemy = "Player",
                    direction = direction,
                    activator = activeBy

                })));


                return;
            }
            //  ai trung dan , firer gui request

            if (niActive.IsControlling() && ni.GetComponent<AiManager>() != null)
            {

                gameObject.SetActive(false);
                networkIdentity.GetSocket().Emit("touchSkill", new JSONObject(JsonUtility.ToJson(new TouchData()
                {
                    id = networkIdentity.GetId(),
                    num = 2,
                    typeId = "003",
                    enemyId = ni.GetId(),
                    typeEnemy = "AI",
                    direction = direction,
                    activator = activeBy


                })));
                return;
            }

        }

    }
}