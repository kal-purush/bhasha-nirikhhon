import { Module } from "@nestjs/common";
import { MongooseModule } from "@nestjs/mongoose";
import { CountriesApiModelService } from "./countriesApi.service";
import { CountriesApiSchema } from "./countriesApi.schema";
import { ViewModels } from "../../helpers/constants";

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: ViewModels.COUNTRIES_API_VIEW, schema: CountriesApiSchema },
    ]),
  ],
  providers: [CountriesApiModelService],
  exports: [CountriesApiModelService],
})
export class CountriesApiModelModule {}