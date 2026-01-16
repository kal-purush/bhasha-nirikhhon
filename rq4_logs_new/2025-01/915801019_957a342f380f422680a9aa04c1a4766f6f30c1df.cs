using FreeView.Sample.Scripts.Controllers;
using FreeView.Sample.Scripts.ViewModels;
using UnityEngine;

namespace FreeView.Sample.Scripts
{
    public class SceneContext : MonoBehaviour
    {
        [SerializeField] private DoorController doorController;

        private static SceneContext _instance;
        private static bool _applicationIsQuitting;
    
        public FreeView.Scripts.FreeView FreeView { get; private set; }

        public static SceneContext GetInstance()
        {
            if (_applicationIsQuitting)
                return null;

            if (_instance == null)
            {
                _instance = FindObjectOfType<SceneContext>();
                if (_instance == null)
                {
                    var obj = new GameObject();
                    obj.name = nameof(SceneContext);
                    _instance = obj.AddComponent<SceneContext>();
                }
            }
            return _instance;
        }
    
        private void Awake()
        {
            if (_instance == null)
                _instance = this;
            else if (_instance != this)
                Destroy(gameObject);
        
            FreeView = new FreeView.Scripts.FreeView(new SampleViewsTemplateSelector());
        }

        private void Start()
        {
            FreeView.Show<PlaygroundViewModel, PlaygroundNavigationArgs>(new PlaygroundNavigationArgs(doorController));
        }

        private void OnApplicationQuit()
        {
            _applicationIsQuitting = true;
        }
    }
}