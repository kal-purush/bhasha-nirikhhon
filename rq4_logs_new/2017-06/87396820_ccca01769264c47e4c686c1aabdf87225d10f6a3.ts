/**
 * Created by Lucas on 5/30/2017.
 */
import {Pipe} from '@angular/core';

@Pipe({
  name: 'filterQuestions'
})
export class FilterQuestions {
  transform(arr, args) {
    if(arr === undefined){return null;}

    if (args == null) {return arr;}

    return arr.filter((item) => {
      // Title Filter
      if (args.selectedTitle) {
        if (item.value.title.toLowerCase().indexOf(args.selectedTitle) == -1) {
          return false;
        }
      }

      // Answers Filter
      if (args.selectedAnswers) {
        if (args.selectedAnswers == "Resolved" && item.value.correctAnswer == null) {
          return false;
        }
        if (args.selectedAnswers == "Unresolved" && item.value.correctAnswer != null) {
          return false;
        }
        if (args.selectedAnswers == "WithAnswers" && !item.value.answers) {
          return false;
        }
      }

      // Friends Filter
      if (args.selectedFriends) {}

      // Language Filter
      if (args.selectedLanguages) {
        let found = false;
        let itemLanguages = Object.keys(item.value.languages);
        let selectedLanguages = Object.keys(args.selectedLanguages);

        for (let i = 0; i < itemLanguages.length && !found; i++) {
          for (let j = 0; j < selectedLanguages.length && !found; j++) {
            found = itemLanguages[i] == selectedLanguages[j];
          }
        }

        if (!found)
          return false;
      }

      // Genre Filter
      if (args.selectedGenres) {
        let found = false;
        let itemGenres = Object.keys(item.value.genres);
        let selectedGenres = Object.keys(args.selectedGenres);

        for (let i = 0; i < itemGenres.length && !found; i++) {
          for (let j = 0; j < selectedGenres.length && !found; j++) {
            found = itemGenres[i] == selectedGenres[j];
          }
        }

        if (!found)
          return false;
      }

      return true;
    });
  }
}