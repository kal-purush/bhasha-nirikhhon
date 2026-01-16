using UnityEngine;
using UnityEngine.UI;
using UnityEngine.Video;

public class VideoController : MonoBehaviour
{
    public VideoClip bigSisterClip;
    public VideoClip littleSisterClip;
    public RawImage bigSisterImage;
    public RawImage littleSisterImage;

    private VideoPlayer bigSisterPlayer;
    private VideoPlayer littleSisterPlayer;

    void Start()
    {
        bigSisterPlayer = SetupVideoPlayer(bigSisterImage, bigSisterClip);
        littleSisterPlayer = SetupVideoPlayer(littleSisterImage, littleSisterClip);

        // Play videos
        bigSisterPlayer.Play();
        littleSisterPlayer.Play();
    }

    private VideoPlayer SetupVideoPlayer(RawImage image, VideoClip clip)
    {
        VideoPlayer player = image.gameObject.AddComponent<VideoPlayer>();
        player.renderMode = VideoRenderMode.MaterialOverride;
        player.targetMaterialProperty = "_MainTex"; // ????????
        player.clip = clip;
        player.isLooping = true; // ??????????

        // ???? targetTexture ??? RawImage ? texture
        image.texture = player.targetTexture;

        return player;
    }
}