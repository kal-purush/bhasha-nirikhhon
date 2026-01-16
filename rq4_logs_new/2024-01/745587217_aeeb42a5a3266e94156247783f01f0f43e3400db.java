    public abstract class Character implements ICharacter {
        protected String name;
        protected int health;
        protected int attack;
        protected int defense;

        private StrategyAttack strategyAttack;

        // Constructor
        public Character(String name, int health, int attack, int defense) {
            this.name = name;
            this.health = health;
            this.attack = attack;
            this.defense = defense;
        }

        public void setStrategy(StrategyAttack strategyAttack) {
            this.strategyAttack = strategyAttack;
        }

        public StrategyAttack getStrategy() {
            return this.strategyAttack;
        }
        public int getHealth() {
            return health;
        }

        public void setHealth(int health) {
            this.health = health;
        }

        public int getAttack() {
            return attack;
        }

        public int getDefense() {
            return defense;
        }

        public String getName() {
            return this.name;
        }

        // Template method for attack
        public final void attack(ICharacter opponent) {
            prepareForAttack(); // Preparation steps (can be overridden by subclasses if needed)
            executeAttackStrategy(opponent); // Delegate the attack execution to the strategy
            finalizeAttack(); // Finalization steps (can be overridden by subclasses if needed)
        }

        // Method to execute the attack strategy
        private void executeAttackStrategy(ICharacter opponent) {
            if (strategyAttack != null) {
                strategyAttack.attack(this, opponent); // Use the strategy to perform the attack
                strategyAttack.applySpecialEffects(this, opponent); // Apply special effects if any
            } else {
                System.out.println(this.getName() + " has no attack strategy set!");
            }
        }

        protected void prepareForAttack() {
            System.out.println(this.getName() + " is preparing for attack...");
        }

        protected void finalizeAttack() {
            System.out.println(this.getName() + " is finalizing attack...");
        }
    }
