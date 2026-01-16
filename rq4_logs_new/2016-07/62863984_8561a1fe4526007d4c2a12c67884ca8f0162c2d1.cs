using UnityEngine;
using System.Collections;

public class cBox : MonoBehaviour {

    public float m_fShakeVelo;

    /// <summary>
    /// Awake() 초기화용 변수
    /// </summary>
    // 속도
    public float m_fSpeed;  // 디폴트 1000부터

	// Use this for initialization
	void Awake() {
	
	}
	
	// Update is called once per frame
	void FixedUpdate () {
        UpdateGravity();
        ViberationCheck();
    }

    void UpdateGravity()
    {
        // 가속도에 따른 중력방향 대한 뱡향 맵핑
        Vector3 dir = Vector3.zero;
        dir.x = Input.acceleration.x;
        dir.y = Input.acceleration.y;
        dir.z = -Input.acceleration.z;

        // 방향 벡터 노멀라이즈
        if (dir.sqrMagnitude > 1)
            dir.Normalize();

        // dt값 적용
        dir *= Time.fixedDeltaTime;

        // 최종 중력방향 변경
        Physics.gravity = dir * 9.8f * m_fSpeed;
    }

    // 진동 체크
    void ViberationCheck()
    {
        Vector3 tempDir = Vector3.zero;
        tempDir = Input.acceleration;
        if (tempDir.sqrMagnitude > m_fShakeVelo) // 흔들렸다!
        {
            // 흔들렸다면 게이지 조절
            cInGameData.Instance.IncreaseGauge();
        }
    }

    public void UpdateSpeed(float _speed)
    {
        m_fSpeed = _speed;
    }

    public void UpdateUserData()
    {

    }
}