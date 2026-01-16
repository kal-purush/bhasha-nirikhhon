import { forwardRef, Module } from "@nestjs/common";
import { ApplicationsController } from "@/jobs/applications/applications.controller";
import { ApplicationsMapper } from "@/jobs/applications/applications.mapper";
import { ApplicationsService } from "@/jobs/applications/applications.service";
import { CommentsMapper } from "@/jobs/applications/comments.mapper";
import { CompaniesModule } from "@/jobs/companies/companies.module";

@Module({
	controllers: [ApplicationsController],
	providers: [ApplicationsService, ApplicationsMapper, CommentsMapper],
	imports: [forwardRef(() => CompaniesModule)],
})
export class ApplicationsModule {
}