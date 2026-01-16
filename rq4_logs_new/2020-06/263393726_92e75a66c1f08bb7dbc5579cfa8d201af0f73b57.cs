using System.Collections;
using UnityEngine;
using Game.Model;
using Game.Misc;

namespace Game
{
    public class GameMediator : MonoBehaviour
    {
        public const int FIELD_WIDTH = 16;
        const float ITERATION_TIME = 0.5f;
        eDirection current_direction = eDirection.RIGHT;
        eDirection ghost_direction = eDirection.DOWN;
        [SerializeField]
        View.VisualManager _visualManager;

        IModelPacMan _model = new ModelPacMan();

        // ====================================

        View.IVisualManager VisualManager => _visualManager;

        // ====================================

        IEnumerator Start()
        {
            VisualManager.Init(_model.EventManager, ITERATION_TIME);

            _model.Init();
            _model.InitGhostA();
            _model.InitGhostB();
            while (true)
            {
                _model.Update(current_direction);
                _model.UpdateGhostA(ghost_direction);
                _model.UpdateGhostB();
                yield return new WaitForSeconds(ITERATION_TIME);
 
            }
        }

        void Update()
        {
            bool isRight = (Input.GetAxis("DPadX") < -0.1f) ? true : false;
            bool isLeft = (Input.GetAxis("DPadX") > 0.1f) ? true : false;
            bool isDown = (Input.GetAxis("DPadY") < -0.1f) ? true : false;
            bool isUp = (Input.GetAxis("DPadY") > 0.1f) ? true : false;

            if (isUp)
            {
                ghost_direction = eDirection.UP;
            }
            if (isDown)
            {
                ghost_direction = eDirection.DOWN;
            }
            if (isRight)
            {
                ghost_direction = eDirection.RIGHT;
            }
            if (isLeft)
            {
                ghost_direction = eDirection.LEFT;
            }

            if (Input.GetKeyDown(KeyCode.UpArrow))
            {
                current_direction = eDirection.UP;
                _visualManager.RotatePacMan(90);
            }
            if (Input.GetKeyDown(KeyCode.DownArrow))
            {
                current_direction = eDirection.DOWN;
                _visualManager.RotatePacMan(270);
            }
            if (Input.GetKeyDown(KeyCode.RightArrow))
            {
                current_direction = eDirection.RIGHT;
                _visualManager.RotatePacMan(0);
            }
            if (Input.GetKeyDown(KeyCode.LeftArrow))
            {
                current_direction = eDirection.LEFT;
                _visualManager.RotatePacMan(180);
            }
        }
    }
}