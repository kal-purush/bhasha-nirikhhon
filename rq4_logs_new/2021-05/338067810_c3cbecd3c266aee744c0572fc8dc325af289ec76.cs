using UnityEngine.SceneManagement;
using QFSW.QC;

namespace Assets.Scripts
{
    [CommandPrefix("SceneLoader.")]
    public static class SceneLoader
    {
        private static void LoadScene(string name)
        {
            SceneManager.LoadScene(name);
        }

        [Command]
        public static void SampleScene() { LoadScene("SampleScene"); }

        [Command]
        public static void MainMenu() { LoadScene("MainMenu"); }

        [Command]
        public static void LevelOne() { LoadScene("Level_1"); }

        [Command]
        public static void LevelTwo() { LoadScene("Level_2"); }
    }
}