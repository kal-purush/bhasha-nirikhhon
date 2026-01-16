import { Pipe, PipeTransform } from 'angular2/core';
import { Album } from './album.model';
import {AlbumListComponent} from './album-list.component';

@Pipe ({
  name: "artist",
  pure: false
})

export class ArtistPipe implements PipeTransform {
  transform(input: Album[], args) {

    //notice the category pipe in task-list.component.ts is taking an array with two values for an argument
    var desiredArtist = args[0][0]; //this is the value of the pipe selector
    var artistList = args[0][1]; // this is an array containing all categories (stock and user created)

    for (var artist of artistList) {
      if (desiredArtist === artist) {
        return input.filter((album) => {
          return (album.artist === artist);
        });
      }
    }
  }
}