export interface WalkModel {
    id: number;
    userId: number;
    dogId: number;
    date: Date;
}

export interface WalkFormValues extends Omit<WalkModel, "id" | "userId" | "dogId" > {}