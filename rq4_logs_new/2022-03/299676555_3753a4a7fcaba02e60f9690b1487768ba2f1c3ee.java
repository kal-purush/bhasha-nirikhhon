package cz.rolling.moirai.assignment.algorithm.stable_matching;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
class Couple {
    private int woman;

    private int man;

    @Override
    public String toString() {
        return "w:" + woman + ",m:" + man;
    }
}