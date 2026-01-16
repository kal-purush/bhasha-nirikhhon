interface Vehicle {
  name: string;
  year: number;
  broken: boolean;
  summary(): string;
}

const oldCivic = {
  name: 'Civic',
  year: 2000,
  broken: true,
  summary(): string {
    return `Name: ${this.name}`;
  },
};

const printVehicle = (vehicle: {
  name: string;
  year: number;
  broken: boolean;
}): void => {
  console.log(`Name: ${vehicle.name}`);
  console.log(`Year: ${vehicle.year}`);
  console.log(`Is broken? ${vehicle.broken}`);
};

const printVehicle2 = (vehicle: Vehicle): void => {
  console.log(`Name: ${vehicle.name}`);
  console.log(`Year: ${vehicle.year}`);
  console.log(`Is broken? ${vehicle.broken}`);
};

const printVehicle21 = (vehicle: Vehicle): void => {
  console.log(vehicle.summary());
};

const printVehicle3 = ({ name, year, broken }: Vehicle): void => {
  console.log(`Name: ${name}`);
  console.log(`Year: ${year}`);
  console.log(`Is broken? ${broken}`);
};

printVehicle(oldCivic);
printVehicle2(oldCivic);
printVehicle21(oldCivic);
printVehicle3(oldCivic);

// A key point is that object should satisfy the interface.
// It means, object must have at least all the defined keys of the target interface,
// while object can have many other keys (object.keys >= interface.keys).
interface Reportable {
  summary(): string;
}

// Using a shared interface to describe the shapes of different objects.
const softDrink = {
  color: 'brwon',
  carbonated: true,
  sugar: 40,
  summary(): string {
    return `My soft drink has ${this.sugar} grams of sugar!.`;
  },
};

const printSummary = ({ summary }: Reportable): void => {
  console.log(summary());
};

printSummary(oldCivic); // Name: Civic
printSummary(softDrink); // My soft drink has 40 grams of sugar!.