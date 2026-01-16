using UnityEngine;


namespace JevLogin
{
    public class Reference
    {
        #region Fields

        private PlayerBall _playerBall;
        private Camera _cameraMain;

        #endregion


        #region Properties

        public PlayerBall PlayerBall
        {
            get
            {
                if (_playerBall == null)
                {
                    var playerObject = Resources.Load<PlayerBall>("Player");
                    _playerBall = Object.Instantiate(playerObject);
                }
                return _playerBall;
            }
        }

        public Camera CameraMain
        {
            get
            {
                if (_cameraMain == null)
                {
                    _cameraMain = Camera.main;
                }
                return _cameraMain;
            }
        }

        #endregion
    }
}