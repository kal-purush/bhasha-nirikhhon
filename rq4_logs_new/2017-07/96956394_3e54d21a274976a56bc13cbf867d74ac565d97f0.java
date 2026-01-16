package com.pokedex.koto.pokedex;

/**
 * Created by KOTO on 18/07/2017.
 */
import java.util.List;


public class Ability {

    private List<Ability_> abilities = null;

    /**
     * No args constructor for use in serialization
     *
     */
    public Ability() {
    }

    /**
     *
     * @param abilities
     */
    public Ability(List<Ability_> abilities) {
        super();
        this.abilities = abilities;
    }

    public List<Ability_> getAbilities() {
        return abilities;
    }

    public void setAbilities(List<Ability_> abilities) {
        this.abilities = abilities;
    }

    public Ability withAbilities(List<Ability_> abilities) {
        this.abilities = abilities;
        return this;
    }

    public class Ability_ {

        private int slot;
        private boolean isHidden;
        private Ability__ ability;

        /**
         * No args constructor for use in serialization
         *
         */
        public Ability_() {
        }

        /**
         *
         * @param isHidden
         * @param slot
         * @param ability
         */
        public Ability_(int slot, boolean isHidden, Ability__ ability) {
            super();
            this.slot = slot;
            this.isHidden = isHidden;
            this.ability = ability;
        }

        public int getSlot() {
            return slot;
        }

        public void setSlot(int slot) {
            this.slot = slot;
        }

        public Ability_ withSlot(int slot) {
            this.slot = slot;
            return this;
        }

        public boolean isIsHidden() {
            return isHidden;
        }

        public void setIsHidden(boolean isHidden) {
            this.isHidden = isHidden;
        }

        public Ability_ withIsHidden(boolean isHidden) {
            this.isHidden = isHidden;
            return this;
        }

        public Ability__ getAbility() {
            return ability;
        }

        public void setAbility(Ability__ ability) {
            this.ability = ability;
        }

        public Ability_ withAbility(Ability__ ability) {
            this.ability = ability;
            return this;
        }

        public class Ability__ {

            private String url;
            private String name;

            /**
             * No args constructor for use in serialization
             *
             */
            public Ability__() {
            }

            /**
             *
             * @param name
             * @param url
             */
            public Ability__(String url, String name) {
                super();
                this.url = url;
                this.name = name;
            }

            public String getUrl() {
                return url;
            }

            public void setUrl(String url) {
                this.url = url;
            }

            public Ability__ withUrl(String url) {
                this.url = url;
                return this;
            }

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public Ability__ withName(String name) {
                this.name = name;
                return this;
            }

        }

    }

}