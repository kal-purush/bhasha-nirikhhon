package com.pokedex.koto.pokedex;

/**
 * Created by interdynamics on 7/26/2017.
 */

public class Ability {
    public Ability() {

    }

    public Ability(int slot, boolean isHidden, Ability_ ability) {
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

    public boolean isHidden() {
        return isHidden;
    }

    public void setHidden(boolean hidden) {
        isHidden = hidden;
    }

    public Ability_ getAbility() {
        return ability;
    }

    public void setAbility(Ability_ ability) {
        this.ability = ability;
    }

    private int slot;
    private boolean isHidden;
    private Ability_ ability;

    public class Ability_ {
        public Ability_(String url, String name) {
            this.url = url;
            this.name = name;
        }
        public Ability_() {

        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        private String url;
        private String name;
    }

}