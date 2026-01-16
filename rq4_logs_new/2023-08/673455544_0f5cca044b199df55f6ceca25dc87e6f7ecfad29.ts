import { IUser } from "../@types";

export interface ICarContext {
	images: IImage[] | [];
	comments: [] | IComment[];
	car: ICar | null;
	allcars: [] | ICar[];
	listCarsUser: [] | ICar[]
	setComments: React.Dispatch<React.SetStateAction<[] | IComment[]>>;
	setImages: React.Dispatch<React.SetStateAction<IImage[] | []>>;
	setCar: React.Dispatch<React.SetStateAction<ICar | null>>;
	setAllCars: React.Dispatch<React.SetStateAction<[] | ICar[]>>;
	setListCarsUser: React.Dispatch<React.SetStateAction<[] | ICar[]>>
	carRegister: (formData: TCarRequest) => Promise<void>;
	editeCar: (formData: TCarUpdate, carId: string) => Promise<void>;
	deleteCar: (carId: string) => Promise<void>;
  }

export interface IDefaultProviderProps {
	children: React.ReactNode
}

export interface ICar {
	"id": string
	"brand": string,
	"model": string,
	"year": string,
	"km": number,
	"color": string,
	"status"?: boolean,
	"fuel": string,
	"price": number,
	"description": string,
	"imgCover": string,
	"bestPrice?": boolean,
	"userId": number,
}

export interface TUserCarsResponse extends IUser {
	"cars": ICar[] | [],
} 

export interface IImage {
	"id": string
	"imgGalery": string,
	"carId": string
}

export interface IComment {
	"id": string
	"description": string,
	"createdAt": string,
	"carId": string,
	"userId": string
}

export type TCarRequest = Omit<ICar, "id" | "userId">

export type TCarUpdate = Partial<TCarRequest>

export interface TCarResponse extends ICar {
	"images": IImage[] | [],
	"comments": IComment[] | [] 
} 

