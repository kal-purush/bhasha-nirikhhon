// Визначте інтерфейс, який використовує сигнатуру індексу з типами об'єднання.
// Наприклад, тип значення для кожного ключа може бути число | рядок.
interface IValues {
  [key: string]: boolean; // we not need there to define key as string | number
}

const val: IValues = {
  val1: true,
  24: false,
  18: true,
};

// Створіть інтерфейс, у якому типи значень у сигнатурі індексу є функціями.
// Ключами можуть бути рядки, а значеннями — функції, які приймають будь-які аргументи.

interface IFunctionAsProperty {
  [key: string]: (...args: any[]) => void;
}

const obj: IFunctionAsProperty = {
  foo1: (arg1: string, arg2: string) => console.log(`Result: ${arg1 + arg2.length}`),

  foo2: (arg: boolean) => console.log(arg ? 'Yes' : 'No'),
  // 33: (arg: boolean) => console.log(arg ? 'Yes' : 'No'),
};

// Опишіть інтерфейс, який використовує сигнатуру індексу для опису об'єкта, подібного до масиву.
// Ключі повинні бути числами, а значення - певного типу.
interface IArrayLikeObject {
  [key: number]: string;
}

const myArrayLikeObject: IArrayLikeObject = {
  0: 'first',
  1: 'second',
  2: 'third',
  // stringTest: 'fifth', // expected error there
};

// Створіть інтерфейс з певними властивостями та індексною сигнатурою.
// Наприклад, ви можете мати властивості типу name: string та індексну сигнатуру для додаткових динамічних властивостей.
interface IContact {
  name: string;
  phone: number;
  [key: string]: boolean | string | number;
}

const me: IContact = {
  name: 'Yuriy',
  phone: 380677022702,
  // phone: false, // expected error
  liveInUa: true,
};

// Створіть два інтерфейси, один з індексною сигнатурою, а інший розширює перший, додаючи специфічні властивості.
// Напишіть функцію, яка отримує об'єкт з індексною сигнатурою і перевіряє,
// чи відповідають значення певних ключів певним критеріям (наприклад, чи всі значення є числами).

interface ILand {
  [key: string]: number | boolean;
}

interface ICottage extends ILand {
  swimmingPool: boolean;
  garage: boolean;
  houseFloorsNumber: number;
}

const cottageBig: ICottage = {
  landSideA: 15,
  landSideB: 25,
  landSideC: 16,
  landSideD: 19,
  landSideE: 21,
  swimmingPool: true,
  garage: true,
  houseFloorsNumber: 3,
};
const cottageSmall: ICottage = {
  landSideA: 15,
  landSideB: true,
  landSideC: 16,
  landSideD: 19,
  landSideE: 21,
  swimmingPool: false,
  garage: false,
  houseFloorsNumber: 1,
};

function objectCheck(testObj: ICottage): boolean {
  for (const key of Object.keys(testObj)) {
    if (
      key !== 'swimmingPool' &&
      key !== 'garage' &&
      key !== 'houseFloorsNumber' &&
      typeof testObj[key] !== 'number'
    ) {
      return false;
    }
  }
  return true;
}

console.log(objectCheck(cottageBig)); // true
console.log(objectCheck(cottageSmall)); // false