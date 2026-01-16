import {Input, OnInit} from '@angular/core';
import {Router} from '@angular/router';
import {Location} from '@angular/common';

import {Component} from '@angular/core';

import {Photo} from '../../models/photo';
import {PhotoService} from '../../services/photo.service';

@Component({
  selector: 'photos-in-album',
  templateUrl: './index.html',
  styleUrls: ['./index.scss']
})

export class PhotosInAlbumComponent implements OnInit {
  /**
   * Input params
   */
  @Input() albumID;

  public photos: Photo[];

  /**
   * Preloader
   */
  public preloader = true;

  /**
   * Is online
   */
  public onLine: boolean;

  /**
   * Photos constructor
   * @param photoService
   * @param router
   * @param location
   */
  public constructor(public photoService: PhotoService,
                     public router: Router,
                     public location: Location) {
    this.onLine = navigator.onLine;
  }

  /****************************************************
   ***************** Live circle events ***************
   ****************************************************/

  /**
   * Init event
   */
  public ngOnInit(): void {
    this.getPhotos();
  }

  /****************************************************
   ********************* Data flow ********************
   ****************************************************/

  /**
   * Get all photos
   */
  private getPhotos(): Promise<Photo[]> {
    this.preloader = true;
    return this.photoService.getPhotoes()
    .then(photos => {
      photos = photos.filter((photo) => photo.albumId === this.albumID);
      this.photos = photos.slice(0, 3);
      this.preloader = false;
      return photos;
    }).catch(() => {
      this.preloader = false;
    });
  }
}