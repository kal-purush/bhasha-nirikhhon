package io.github.krakowski.challenge;

public @interface Difficulty {

    Level value();

    enum Level {

        NONE(0),
        EASY(1),
        MEDIUM(2),
        HARD(3),
        SPECIAL(4);

        private final int points;

        Level(int points) {
            this.points = points;
        }

        public int getPoints() {
            return points;
        }
    }
}