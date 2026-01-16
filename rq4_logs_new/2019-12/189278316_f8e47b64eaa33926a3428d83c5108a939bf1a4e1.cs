using UnityEngine;

namespace Screeps3D.RoomObjects.Views
{
    public class FactoryView : MonoBehaviour, IObjectViewComponent
    {
        [SerializeField] private Renderer rend;
        [SerializeField] private Animator anim;

        private Factory _factory;
        // TODO: we also need the mineral on the location to get regen time if we want to do something specific in regards to that

        public void Init()
        {
        }

        public void Load(RoomObject roomObject)
        {
            _factory = roomObject as Factory;
        }

        public void Delta(JSONObject data)
        {
        }

        public void Unload(RoomObject roomObject)
        {
            _factory = null;
        }

        private void Update()
        {
            if (_factory == null)
                return;
            
            // TODO: actions, like creep
        }
    }
}