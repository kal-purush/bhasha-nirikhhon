import { exerciseCategories, sounds } from "./sounds";
import { shuffle } from "./utils";

export class Orchestrator {
    public hasGone: boolean;
    private exercises: string[];
    private audioObj: HTMLAudioElement;
    constructor(audioObj: HTMLAudioElement) {
        this.exercises = this.getExercisePaths();
        this.audioObj = audioObj;
        this.hasGone = false;
    }

    public go() {
        this.hasGone = true;
        this.playSequence(this.getFullSequence());
    }

    // Gets the files paths of the excercises in order
    private getExercisePaths(): string[] {
        const paths: string[] = [];

        // Randomize the different categories, but start with cardio
        const shuffledCategories = shuffle(Object.keys(sounds.excercises).slice().filter(key => key !== exerciseCategories.cardio));
        shuffledCategories.unshift(exerciseCategories.cardio)
        const exerciseGroups = shuffledCategories.map(category => shuffle(sounds.excercises[category]));

        // Build an excercise list using one from each category for each round, in order of categories
        for(let i = 0; i < 3; i++) {
            exerciseGroups.forEach(exerciseGroup => {
                if(exerciseGroup.length > 1){
                    paths.push(exerciseGroup.shift())
                } else if (exerciseGroup.length === 1){
                    paths.push(exerciseGroup[0])
                }
            });
        }
        return paths;
    }

    // Gets the sequence of sounds needed for the workout,
    // including the transition sounds and breaks.
    private getFullSequence() {
        const sequence = [sounds.transitions.welcome];
        this.exercises.forEach(excercise => {
            sequence.push(...this.getSingleExerciseSequence(excercise));
        });
        sequence.push(sounds.transitions.complete);
        return sequence;
    }

    // Gets one exercise sequence, including intro sounds and silence
    private getSingleExerciseSequence(exercisePath: string) {
        return [
            sounds.transitions.next,
            exercisePath,
            sounds.transitions.silence5,
            exercisePath,
            sounds.transitions.go,
            sounds.transitions.silence30,
            sounds.transitions.rest
        ]
    }

    // Plays a sequence of audio paths
    private playSequence(soundPaths: string[]) {
        const newSoundList = soundPaths.slice();
        const sound = newSoundList.shift();
        if (sound) {
          this.audioObj.src = sound;
          this.audioObj.onended = () => {
            this.playSequence(newSoundList);
          }
          this.audioObj.play();
        }
    }
}