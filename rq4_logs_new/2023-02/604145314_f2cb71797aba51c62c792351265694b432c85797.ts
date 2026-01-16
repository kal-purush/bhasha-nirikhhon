type HasId = {
  id: string;
};

export type ProtoMarsPhotoStructure = {
  sol: string;
  camera_id: string;
  camera_name: string;
  camera_rover_id: string;
  camera_full_name: string;
  img_src: string;
  earth_date: string;
  rover_id: string;
  rover_name: string;
  rover_landing_date: string;
  rover_launch_date: string;
  rover_status: string;
  apiOrigin: 'APIPublic' | 'APIPrivate';
  isFavorite: boolean;
  favoriteName: string;
};

export type MarsPhotoStructure = HasId & ProtoMarsPhotoStructure;

export class MarsPhoto implements MarsPhotoStructure {
  id: string;
  sol: string;
  camera_id: string;
  camera_name: string;
  camera_rover_id: string;
  camera_full_name: string;
  img_src: string;
  earth_date: string;
  rover_id: string;
  rover_name: string;
  rover_landing_date: string;
  rover_launch_date: string;
  rover_status: string;
  apiOrigin: 'APIPublic' | 'APIPrivate';
  isFavorite: boolean;
  favoriteName: string;
  constructor(
    id: string,
    sol: string,
    camera_id: string,
    camera_name: string,
    camera_rover_id: string,
    camera_full_name: string,
    img_src: string,
    earth_date: string,
    rover_id: string,
    rover_name: string,
    rover_landing_date: string,
    rover_launch_date: string,
    rover_status: string,
    apiOrigin: 'APIPublic' | 'APIPrivate',
    isFavorite: boolean,
    favoriteName: string
  ) {
    this.id = id;
    this.sol = sol;
    this.camera_id = camera_id;
    this.camera_name = camera_name;
    this.camera_rover_id = camera_rover_id;
    this.camera_full_name = camera_full_name;
    this.img_src = img_src;
    this.earth_date = earth_date;
    this.rover_id = rover_id;
    this.rover_name = rover_name;
    this.rover_landing_date = rover_landing_date;
    this.rover_launch_date = rover_launch_date;
    this.rover_status = rover_status;
    this.apiOrigin = apiOrigin;
    this.isFavorite = isFavorite;
    this.favoriteName = favoriteName;
  }
}