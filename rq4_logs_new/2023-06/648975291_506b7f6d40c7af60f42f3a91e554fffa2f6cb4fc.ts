import { useState, useEffect } from 'react'
interface GeolocationPosition {
  coords: {
    latitude: number
    longitude: number
  }
}

interface GeolocationPositionError {
  code: number
  message: string
}

interface locationCoord {
  loaded: boolean
  coordinates: {
    latitude: number
    longitude: number
  }
  error?: {
    code: number
    message: string
  }
}

const useGeoLocation = () => {
  const [locationCoord, setLocationCoord] = useState<locationCoord>({
    loaded: false,
    coordinates: { latitude: 0, longitude: 0 },
  })

  const onSuccess = (location: GeolocationPosition) => {
    setLocationCoord({
      loaded: true,
      coordinates: { latitude: location.coords.latitude, longitude: location.coords.longitude },
    })
  }
  const onError = (error: GeolocationPositionError) => {
    setLocationCoord((prev) => ({
      ...prev,
      loaded: true,
      error,
    }))
  }

  useEffect(() => {
    if (!('geolocation' in navigator)) {
      onError({
        code: 0,
        message: 'Geolocation not supported',
      })
      setLocationCoord((prev) => ({
        ...prev,
        loaded: true,
        error: {
          code: 0,
          message: 'Geolocation not supported',
        },
      }))
    }
    navigator.geolocation.getCurrentPosition(onSuccess, onError)
  }, [])

//   function initMap(): void {
//     const map = new google.maps.Map(
//       document.getElementById("map") as HTMLElement,
//       {
//         zoom: 8,
//         center: { lat: 40.731, lng: -73.997 },
//       }
//     );
//     const geocoder = new google.maps.Geocoder();
//     const infowindow = new google.maps.InfoWindow();
  
//     (document.getElementById("submit") as HTMLElement).addEventListener(
//       "click",
//       () => {
//         geocodeLatLng(geocoder, map, infowindow);
//       }
//     );
//   }
  
//   function geocodeLatLng(
//     geocoder: google.maps.Geocoder,
//     map: google.maps.Map,
//     infowindow: google.maps.InfoWindow
//   ) {
//     const input = (document.getElementById("latlng") as HTMLInputElement).value;
//     const latlngStr = input.split(",", 2);
//     const latlng = {
//       lat: parseFloat(latlngStr[0]),
//       lng: parseFloat(latlngStr[1]),
//     };
  
//     geocoder
//       .geocode({ location: latlng })
//       .then((response) => {
//         if (response.results[0]) {
//           map.setZoom(11);
  
//           const marker = new google.maps.Marker({
//             position: latlng,
//             map: map,
//           });
  
//           infowindow.setContent(response.results[0].formatted_address);
//           infowindow.open(map, marker);
//         } else {
//           window.alert("No results found");
//         }
//       })
//       .catch((e) => window.alert("Geocoder failed due to: " + e));
//   }
  
//   declare global {
//     interface Window {
//       initMap: () => void;
//     }
//   }
//   window.initMap = initMap;

  //  when the component is mounted fetch the location if there is geolocation in the browser

  return { locationCoord }
}
export default useGeoLocation