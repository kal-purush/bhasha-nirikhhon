using Cinemachine;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using UnityEngine;
using UnityEngine.InputSystem;

public class MirrorPuzzleDoor : SlideDoor
{
    public RayGame Game;
    public CinemachineVirtualCamera VCam;

    private void Start() {
        Game.OnDestRayHitted += EndGame;
    }

    async public void EndGame(){
        GamePhaseManager.instance.SwitchCutscene(true);
        Time.timeScale = 0f;
        await Task.Delay(500);

        Time.timeScale = 1f;
        VCam.Priority = 20;
        await Task.Delay(1000);
        Time.timeScale = 0f;

        LineRenderer linerenderer = Game.lineRenderer;
        Destroy(Game);
        Destroy(linerenderer);
        Open();
        await Task.Delay((int)MoveDuration * 1000 + 1000);

        VCam.Priority = 10;
        GamePhaseManager.instance.SwitchCutscene(false);
        Time.timeScale = 1f;
    }

    public void OnDestroy() {
        Game.OnDestRayHitted -= EndGame;
    }
}