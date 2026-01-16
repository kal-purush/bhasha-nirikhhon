using UnityEngine;

public class EndPoint : MonoBehaviour
{
    private StartPoint startPoint;

    void Start()
    {
        // Tìm StartPoint trong scene
        startPoint = FindObjectOfType<StartPoint>();
    }

    void OnTriggerEnter(Collider other)
    {
        // Nếu player va chạm với EndPoint, dừng hẳn bộ đếm thời gian
        if (other.CompareTag("Player") && startPoint != null)
        {
            startPoint.StopTimer();

            // Kiểm tra nếu người chơi hoàn thành trước khi hết thời gian
            if (startPoint.timer < startPoint.timeLimit)
            {
                Debug.Log("Bạn đã hoàn thành trước khi hết thời gian!");
            }
            else
            {
                Debug.Log("Bạn đã hết thời gian!");
            }
        }
    }
}