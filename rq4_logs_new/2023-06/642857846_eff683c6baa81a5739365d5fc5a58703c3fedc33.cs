namespace Game
{
    class NotJumpable : IJumpable
    {
        public bool isJumping { get; set; }
        public int jumpStage { get; set; }

        public void jump()
        {
            System.Console.WriteLine("I cannot jump!");
        }
        public void jump(Dictionary<string, int> point) { }
    }
}