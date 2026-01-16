import { Module } from "@nestjs/common";
import { ConfigModule } from "@nestjs/config";
import { SequelizeModule } from "@nestjs/sequelize";
import { AppController } from "src/app/app.controller";
import { AppService } from "src/app/app.service";
import { getDBConfig } from "src/db/config";
import { ApplicationsModule } from "@/applications/applications.module";
import { CompaniesModule } from "@/companies/companies.module";

@Module({
	imports: [
		ApplicationsModule,
		CompaniesModule,
		ConfigModule.forRoot({
			envFilePath: [".env.local", ".env"],
		}),
		SequelizeModule.forRoot(getDBConfig()),
	],
	controllers: [AppController],
	providers: [AppService],
})
export class AppModule {
}