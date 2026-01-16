export const getUserLocation = (): Promise<[number, number]> => {
  return new Promise((resolve) => {
    if (!navigator.geolocation) {
      console.log("Geolocation is not supported by this browser. Using default location (Madrid)")
      resolve([40.41672071780762, -3.703395193582358]) // Madrid como fallback
      return
    }

    navigator.geolocation.getCurrentPosition(
      (position) => {
        const { latitude, longitude } = position.coords
        resolve([latitude, longitude])
      },
      (error) => {
        console.error("Error getting location:", error)
        console.log("Unable to get location. Using default location (Madrid).")
        resolve([40.41672071780762, -3.703395193582358]) // Madrid como fallback
      }
    )
  })
}