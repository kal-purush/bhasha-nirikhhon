import {
  Arg,
  Authorized,
  Field,
  InputType,
  Int,
  Mutation,
  Query,
  Resolver,
} from "type-graphql";
import { BusinessField, Municipality } from "../entity";
import { RegistrationRequest } from "../entity/RegistrationRequest";

@InputType()
class RegistrationRequestInput implements Partial<RegistrationRequest> {
  @Field()
  lastName: string;

  @Field()
  firstName: string;

  @Field()
  email: string;

  @Field()
  businessID: string;

  @Field(() => Int, { nullable: true })
  businessFieldID: number;

  @Field(() => Int, { nullable: true })
  municipalityID: number;
}

@Resolver(RegistrationRequest)
export class RegistrationRequestResolver {
  @Authorized([])
  @Query(() => [RegistrationRequest])
  allRegistrationRequests() {
    return RegistrationRequest.find({
      relations: ["municipality", "businessField"],
    });
  }

  @Mutation(() => RegistrationRequest)
  async createRegistrationRequest(
    @Arg("data")
    {
      lastName,
      firstName,
      email,
      businessID,
      businessFieldID,
      municipalityID,
    }: RegistrationRequestInput
  ): Promise<RegistrationRequest | undefined> {
    const businessField = await BusinessField.findOne(businessFieldID);
    if (!businessField) return undefined;

    const municipality = await Municipality.findOne(municipalityID);
    if (!municipality) return undefined;

    const newRequest = await RegistrationRequest.create({
      lastName,
      firstName,
      email,
      businessID,
      businessField,
      municipality,
    }).save();
    console.log("new request created", newRequest);

    return newRequest;
  }
}