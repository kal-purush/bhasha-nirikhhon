/**
 * Created by dhnishi on 4/25/15.
 */

/// <reference path="_all.ts"/>

module battlejack {
    var BASE_EXPERIENCE = 1000;

    export var getExperienceToNextLevel = (currentLevel : number) => {
        return (Math.pow(currentLevel, 2) + currentLevel) / 2 * BASE_EXPERIENCE;
    };

    export var getExperienceNeededForLevel = (level : number) => {
        return (Math.pow(level, 2) - level) / 2 * BASE_EXPERIENCE;
    };
}