using UnityEngine;
using System.Collections;

public class Util : MonoBehaviour
{
    /// <summary>
    /// GameObject가 화면 안에 있는지 여부를 판별
    /// </summary>
    /// <param name="obj"></param>
    /// <returns></returns>
    public static bool IsInScreen(GameObject obj)
    {
        Vector3 targetScreenPos = Camera.main.WorldToScreenPoint(obj.transform.position);
        if (targetScreenPos.x > Screen.width ||
            targetScreenPos.x < 0 ||
            targetScreenPos.y > Screen.height ||
            targetScreenPos.y < 0)
        {
            return false;
        }
        return true;
    }

    /// <summary>
    /// Vector2를 degree 각만큼 시계방향으로 회전
    /// </summary>
    /// <param name="origin">회전전 벡터</param>
    /// <param name="deg">회전할 각도(degree값)</param>
    /// <returns>회전한 Vector2</returns>
    public static Vector2 RotateVector2(Vector2 origin, float deg)
    {
        float rad = deg * Mathf.Deg2Rad;
        float cos = Mathf.Cos(rad);
        float sin = Mathf.Sin(rad);
        float x = (origin.x * cos) - (origin.y * sin);
        float y = (origin.x * sin) + (origin.y * cos);

        return new Vector2(x, y);
    }

    /// <summary>
    /// 문자열 마지막 글자에 받침유무를 판별.
    /// </summary>
    /// <param name="str">입력 문자열</param>
    /// <returns>마지막 글자에 받침이 있으면 true</returns>
    public static bool CheckEndHangul(string str)
    {
        char[] carray = str.ToCharArray();
        char last = carray[carray.Length - 1];
        if (last >= 0xAC00 && last <= 0xD7A3)
        {
            Debug.Log("한글문자");
            if (((last - 0xAC00) % 28) == 0)
            {
                Debug.Log("받침없음");
                return false;
            }
            else
            {
                Debug.Log("받침있음");
                return true;
            }
        }
        else
        {
            Debug.Log("한글문자아님");
            return false;
        }
    }
}