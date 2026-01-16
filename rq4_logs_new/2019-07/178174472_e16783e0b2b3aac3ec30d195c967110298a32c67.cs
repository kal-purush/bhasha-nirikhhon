using MononokeEngine.ECS;
using MononokeEngine.Graphics;
using MononokeEngine.Input;
using MononokeEngine.Scenes;

namespace MononokeEngine
{
    public static class Mononoke
    {

        public static SceneManager Scenes
        {
            get
            {
                if (_scenes == null)
                    _scenes = new SceneManager();
                return _scenes;
            }
        }
        private static SceneManager _scenes;
        
        
        
        
        public static EcsManager Ecs
        {
            get
            {
                if (_ecs == null)
                    _ecs = new EcsManager();
                return _ecs;
            }
        }
        private static EcsManager _ecs;
        
        
        
        public static InputManager Input
        {
            get
            {
                if (_input == null)
                    _input = new InputManager();
                return _input;
            }
        }
        private static InputManager _input;
        
        
        
        public static GraphicsManager Graphics
        {
            get
            {
                if (_graphics == null)
                    _graphics = new GraphicsManager();
                return _graphics;
            }
        }
        private static GraphicsManager _graphics;
        
        
        
    }



    
}