using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class HealthBar : MonoBehaviour
{
    [SerializeField] private Slider healthBar;
    [SerializeField] private Slider backHealthBar;
    [SerializeField] private float speed = 1f;
    [SerializeField, ReadOnly] private int currentHealth = 1000;
    [SerializeField, ReadOnly] private int delayHealth = 1000;
    [SerializeField, ReadOnly] private Coroutine backBarCoroutine;

    #region Unity Method
        private void Awake(){
            // slider = GetComponent<Slider>();
            SetMaxHealth(healthBar ,currentHealth);
            SetMaxHealth(backHealthBar, currentHealth);

        }

        private void Update(){
            // if (delayHealth > currentHealth){
            //     delayHealth -= 1;
            //     SetHealth(backHealthBar, delayHealth);
            // } else if (delayHealth < currentHealth){
            //     delayHealth = currentHealth;
            // }
        }
    #endregion

    #region HealthBar Manager
        public void SetMaxHealth(Slider slider,int health){
            slider.maxValue = health;
            slider.value = health;
        }

        public void SetHealth(Slider slider, int health){
            slider.value = health;
        }
    #endregion

    public void TestHealth(int health){
        currentHealth += health;
        currentHealth = Mathf.Clamp(currentHealth, 0, 1000); // Giới hạn giá trị từ 0 đến 1000
        SetHealth(healthBar, currentHealth);

         if (backBarCoroutine != null)
        {
            StopCoroutine(backBarCoroutine); // Hủy Coroutine hiện tại nếu có
        }
        backBarCoroutine = StartCoroutine(AnimateBackHealth());

        // if (currentHealth < 0) currentHealth = 0;
        // else if (currentHealth > 1000) currentHealth = 1000;
        // SetHealth(healthBar, currentHealth);
    }

    private IEnumerator AnimateBackHealth()
    {
        while (delayHealth != currentHealth) // Tiếp tục chạy khi delayHealth chưa bằng currentHealth
        {
            if (delayHealth > currentHealth){
                // Giảm dần delayHealth để khớp với currentHealth
                delayHealth -= Mathf.CeilToInt(speed * Time.deltaTime * 100);
                delayHealth = Mathf.Max(delayHealth, currentHealth); // Đảm bảo không giảm thấp hơn currentHealth
            } else if (delayHealth < currentHealth) {
                // Tăng dần delayHealth để khớp với currentHealth
                delayHealth += Mathf.CeilToInt(speed * Time.deltaTime * 100);
                delayHealth = Mathf.Min(delayHealth, currentHealth); // Đảm bảo không vượt quá currentHealth
            }
            SetHealth(backHealthBar, delayHealth);
            yield return null;
        }
    }
}