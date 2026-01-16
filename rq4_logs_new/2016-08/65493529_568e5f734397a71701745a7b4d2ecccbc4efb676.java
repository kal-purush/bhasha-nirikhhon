package com.coder.guider.builder;

/**
 * Created by apple on 16/8/15.
 */
public class Cat {

    Builder builder;

    public class Builder {

        private String name;
        private int weight;

        public String getName() {
            return name;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public int getWeight() {
            return weight;
        }

        public Builder setWeight(int weight) {
            this.weight = weight;
            return this;
        }

        public Cat create() {
            Cat cat = new Cat();
            cat.builder = this;
            return cat;
        }

    }

}
