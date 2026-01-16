using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerShooting : MonoBehaviour{
    [SerializeField] private float attackCoolDown;
    [SerializeField] private Transform firePoint;
    [SerializeField] private GameObject[] fireballs;
    [SerializeField, ReadOnly] private float coolDownTimer = Mathf.Infinity;
    [SerializeField, ReadOnly] private PlayerController playerController;

    #region Unity Method
        private void Awake(){
            playerController = GetComponent<PlayerController>();
        }

        private void Update(){
            if (Input.GetMouseButton(0) && coolDownTimer > attackCoolDown && playerController.canAttack())
                Attack();

            coolDownTimer += Time.deltaTime;
        }

    #endregion

    #region Shooting Function
        private void Attack(){
            coolDownTimer = 0;
            print("Shoot");

            // Chạy animation Shoot
            playerController.PlayAnimation(0, "Shoot", false);

            // Sau khi kết thúc animation Shoot, quay lại Idle hoặc Run
            StartCoroutine(ResetAnimation());

            fireballs[FindFireball()].transform.position = firePoint.position;
            fireballs[FindFireball()].GetComponent<Projectile>().SetDirection(Mathf.Sign(transform.localScale.x));
        }


        private IEnumerator ResetAnimation() {
            yield return new WaitForSeconds(0.3f); // Thời gian tương đương với độ dài của hoạt ảnh Shoot

            // Quay lại trạng thái phù hợp
            if (Mathf.Abs(playerController.horizontalInput) > 0.01f) {
                playerController.PlayAnimation(0, "Run", true);
            } else {
                playerController.PlayAnimation(0, "Idle", true);
            }
        }

        private int FindFireball(){
            for (int i = 0; i < fireballs.Length; i++){
                if (!fireballs[i].activeInHierarchy){
                    return i;
                }
            }
            return 0;
        }

    #endregion

}