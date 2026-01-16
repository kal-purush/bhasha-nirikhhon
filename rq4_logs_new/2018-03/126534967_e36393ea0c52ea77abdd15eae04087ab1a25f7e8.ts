export class Me {
  firstName: string;
  middleName: string;
  lastName: string;
  email: string;
  phone: number;
  linkedin: { icon: string; url: string };
  twitter: { icon: string; url: string };
  instagram: { icon: string; url: string };
  github: { icon: string; url: string };
  location: {
    city: string;
    country: string;
    coordinates: { lat: number; lng: number };
  };
  company: { name: string; url: string };
  languages: string[];
  illustratedPic: string;
  livePic: string;
  freelanceStatus: string;
  welcome: string;
  beliefs: string;
  goals: string;
  story: string;
}