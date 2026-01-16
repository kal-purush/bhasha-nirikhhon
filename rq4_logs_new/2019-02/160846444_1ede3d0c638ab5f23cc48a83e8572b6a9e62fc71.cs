namespace TreehouseDefense {

    class Ressurecting_Invader : IInvader {

        private Basic_Invader _incarnation1;
        private Strong_Invader _incarnation2;

        public MapLocation Location => _incarnation1.IsNeutralized ? _incarnation2.Location : _incarnation1.Location;

        public bool HasScored => _incarnation1.HasScored || _incarnation2.HasScored;

        public int Health => _incarnation1.IsNeutralized ? _incarnation2.Health : _incarnation1.Health;

        public bool IsNeutralized => _incarnation1.IsNeutralized && _incarnation2.IsNeutralized;

        public bool IsActive => !(IsNeutralized || HasScored);

        public Ressurecting_Invader (Path path) {

            _incarnation1 = new Basic_Invader (path);
            _incarnation2 = new Strong_Invader (path);

        }

        public void Move () {
            _incarnation1.Move ();
            _incarnation2.Move ();
        }

        public void DecreaseHealth (int factor) {
            if (!_incarnation1.IsNeutralized) {
                _incarnation1.DecreaseHealth (factor);
            }
            else {
                _incarnation2.DecreaseHealth (factor);
            }
        }

    }
}