namespace devices
{
    public class device
    {
        public string name;
        public string type;
        public int id;
        public bool status;
        public bool operation = false;

        public device(string name, string type, int id)
        {
            this.name = name;
            this.type = type;
            this.id = id;
        }
    }
}