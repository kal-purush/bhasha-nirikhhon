import Root from './component/Root';
import Avatar from './component/Avatar';
import Map from './component/Map';

let POKEMON = {
  Root: Root,
  Avatar: Avatar,
  Map: Map
};

(<any>window).POKEMON = POKEMON;