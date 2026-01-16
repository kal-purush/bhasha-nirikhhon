interface Cardata{
    id:number;
    brand:string;
    Acceleration:string;
    CO2Emissions: string;
    star:number;
      topSpeed: string;
      Gears: string;
      Transmission: string;
      batteryCapacity: string;
      rating: number
      imgUrl: string;
      price: number
      features:any;
      description:string;
  }

const bmw = require("../asset/car-images/bmw-offer.png");
const mercedes = require("../asset/car-images/mercedes-offer.png");
const nissan = require("../asset/car-images/nissan-offer.png");
const toyota = require("../asset/car-images/offer-toyota.png");

export const CarDatas: Cardata[] = [
  {
    id: 1,
    brand: "Toyota",
    Acceleration: "7.4 sec",
    CO2Emissions: "98 g/km",
    topSpeed: "111 mph",
    Gears: "1-speed",
    Transmission: "Automatic",
    batteryCapacity: "1.8 kWh",
    rating: 132,
    star: 5.0,
    imgUrl: toyota,
    price: 65,
    features: [
      "Leather Seats",
      '"7” Display with Toyota Touch 2 Multimedia, Bluetooth and DAB',
      "7” TFT Multi Information Display",
      "Rear Cross Traffic Alert",
      "Dual Zone Air Conditioning",
      "Smart Entry and Push Button Start",
      "Wireless Charger",
      "Toyota Safety Sense",
      "Wireless Charger",
      "Heated Driver and Passenger Seat",
    ],
    description:
      " The Camry Hybrid seamlessly blends the sophisticated luxury of a premium sedan with the impressive capabilities of a self-charging hybrid engine. Its sleek appearance and cutting-edge features create an exhilarating and inventive driving experience. The 2.5-liter engine delivers remarkable power and torque, ensuring smooth performance while maintaining a blissfully quiet ride and improved efficiency.",
  },

  {
    id: 2,
    brand: "BMW",
    Acceleration: "8.3 sec",
    CO2Emissions: "134 g/km",
    topSpeed: "139 mph",
    Gears: "8-speed",
    star: 4.29,
    Transmission: "Automatic",
    batteryCapacity: "12 kWh",
    rating: 132,
    imgUrl: bmw,
    price: 65,
    features: [
      "Heated front seats",
      "Front and rear headrests",
      "M Sport multifunction leather steering wheel",
      "iDrive controller on centre console",
      "Cruise control with brake function + speed limiter",
      "Optimum shift indicator",
      "Child proof locking system in rear door",
      "Driver/Front Passenger airbags",
      "Active guard plus",
      "Front sports seats",
    ],
    description:
      "The Sport 3 Series comes with 18-inch alloy wheels, along with exterior trim details and leather seats. Additionally, it boasts the SE's LED headlights, three-zone climate control, automatic cruise control, heated seats, automatic headlights and wipers, front and rear parking sensors, a reversing camera, and BMW's 8.8-inch screen infotainment system, which includes DAB radio, Bluetooth, USB connection, Apple CarPlay, and Android Auto.",
  },

  {
    id: 3,
    brand: "Nissan",
    Acceleration: "7.5 sec",
    CO2Emissions: "115 g/km",
    topSpeed: "139 mph",
    Transmission: "Automatic",
    star: 4.99,
    batteryCapacity: "63 - 87 kWh",
    Gears: "1-speed",
    rating: 132,
    imgUrl: nissan,
    price: 65,
    features: [
      "Electrically driven intelligent braking system",
      "Intelligent Cruise control (ICC)",
      "Heated front and rear seats",
      "Power sliding centre console",
      "Max DC charging power: up to 130kW",
      "Auto parking system",
      "Driver/Front Passenger airbags",
      "Rear armrest",
      "Intelligent rear emergency braking with pedestrian recognition",
    ],
    description:
      "Included as standard, the Nissan Ariya in Advance specification comes equipped with 19-inch alloy wheels, LED headlights, climate control, a heated windscreen, power-adjustable and heated front seats, a 12.3-inch touchscreen infotainment system, and front and rear parking sensors. Moreover, the safety features on this trim are quite impressive, encompassing blind spot monitoring, adaptive cruise control, a driver drowsiness monitor, and autonomous emergency braking.",
  },

  {
    id: 4,
    brand: "Mercedes",
    Acceleration: "5.2 sec",
    CO2Emissions: "118 g/km",
    topSpeed: "155 mph",
    Transmission: "Automatic",
    batteryCapacity: "13.5 kWh",
    Gears: "9-speed",
    star: 5.0,
    rating: 132,
    imgUrl: mercedes,
    price: 85,
    features: [
      "Anti-lock brake system",
      "Warning triangle and first aid kit",
      "Window airbags",
      "Head up Display",
      "Outside temperature gauge",
      "Active park assist",
      "Black roof liner",
      "Steering wheel gearshift paddles",
      "Lockable/illuminated air conditioned glovebox",
    ],
    description:
      "The long and sleek appearance of the vehicle is accentuated by the 20-inch alloy wheels, and the style is further intensified with the addition of an AMG bodykit. Stepping inside, you'll find a slide and tilt glass sunroof as well as privacy glass. Completing the package is a range of convenient features, including automatic lights and wipers, cruise control, and multibeam LED headlights, creating a well-rounded and comprehensive offering.",
  },
];