using System.Collections;
using UnityEngine;
using UnityEngine.Networking;

public class API : MonoBehaviour
{
    private string baseUrl = "http://localhost:3001";
    private string playerId = "yourPlayerId"; // Đặt ID của người chơi ở đây

    // Lưu dữ liệu checkpoint lên server
    public IEnumerator SaveCheckpoint(Vector3 position)
    {
        string url = $"{baseUrl}/save-checkpoint";

        // Tạo dữ liệu JSON để gửi đi
        var checkpointData = new
        {
            playerId = "1",
            x_position = position.x,
            y_position = position.y,
            z_position = position.z
        };

        string jsonData = JsonUtility.ToJson(checkpointData);
        UnityWebRequest request = new UnityWebRequest(url, "POST");
        byte[] jsonToSend = new System.Text.UTF8Encoding().GetBytes(jsonData);
        request.uploadHandler = new UploadHandlerRaw(jsonToSend);
        request.downloadHandler = new DownloadHandlerBuffer();
        request.SetRequestHeader("Content-Type", "application/json");

        yield return request.SendWebRequest();

        if (request.result == UnityWebRequest.Result.Success)
        {
            Debug.Log("Lưu checkpoint thành công: " + request.downloadHandler.text);
        }
        else
        {
            Debug.LogError("Lỗi khi lưu checkpoint: " + request.error);
        }
    }

    // Lấy checkpoint cuối cùng từ server
    public IEnumerator GetLastCheckpoint(System.Action<Vector3> onSuccess)
    {
        string url = $"{baseUrl}/last-checkpoint?playerId={playerId}";
        UnityWebRequest request = UnityWebRequest.Get(url);

        yield return request.SendWebRequest();

        if (request.result == UnityWebRequest.Result.Success)
        {
            CheckpointData checkpoint = JsonUtility.FromJson<CheckpointData>(request.downloadHandler.text);
            Vector3 lastPosition = new Vector3(checkpoint.checkpoint.x_position, checkpoint.checkpoint.y_position, checkpoint.checkpoint.z_position);

            // Ghi log vị trí nhận được từ máy chủ
            Debug.Log("Vị trí checkpoint nhận được từ máy chủ: " + lastPosition);

            onSuccess(lastPosition);
        }
        else
        {
            Debug.LogError("Lỗi khi lấy checkpoint: " + request.error);
            onSuccess(Vector3.zero);
        }
    }

    [System.Serializable]
    private class CheckpointData
    {
        public string message;
        public Checkpoint checkpoint;

        [System.Serializable]
        public class Checkpoint
        {
            public string playerId;
            public float x_position;
            public float y_position;
            public float z_position;
            public string timestamp;
        }
    }
}