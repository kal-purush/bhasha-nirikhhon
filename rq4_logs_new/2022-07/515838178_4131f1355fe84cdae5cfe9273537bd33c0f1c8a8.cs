using UnityEngine.SceneManagement;

namespace Overflow.General
{
    public static class SceneController
    {
        public static void LoadScene(SceneEnum scene)
        {
            SceneManager.LoadSceneAsync($"_Scenes/{scene}");
        }
    }
}