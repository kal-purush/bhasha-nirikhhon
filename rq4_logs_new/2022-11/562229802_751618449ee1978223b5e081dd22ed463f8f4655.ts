import { IsNotEmpty } from "class-validator";

export class CreateSubscriptionsDto {

    @IsNotEmpty()
	businessId: number;
	
    @IsNotEmpty()
	applicationId: number;
	
    @IsNotEmpty()
	subscriptionKey: string;
	
    @IsNotEmpty()
	planType: number;
	
    @IsNotEmpty()
	amount: number;
	
    @IsNotEmpty()
	currency: string;

	startDate: Date;

	endDate: Date;

	createdBy: string;

	created_at: Date; // Creation date

	updated_at: Date; // Last updated date

}