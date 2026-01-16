import { CommonModule } from '@angular/common'
import { CUSTOM_ELEMENTS_SCHEMA, NgModule } from '@angular/core'
import { FormsModule, ReactiveFormsModule } from '@angular/forms'
import { RouterModule } from '@angular/router'
import { LetDirective } from '@ngrx/component'
import { EffectsModule } from '@ngrx/effects'
import { StoreModule } from '@ngrx/store'
import { TranslateModule } from '@ngx-translate/core'
import { addInitializeModuleGuard } from '@onecx/angular-integration-interface'
import { PortalCoreModule, providePortalDialogService } from '@onecx/portal-integration-angular'
import { CalendarModule } from 'primeng/calendar'
import { SharedModule } from '../shared/shared.module'
import { AIKnowledgeVectorDbFeature } from './ai-knowledge-vector-db.reducers'
import { routes } from './ai-knowledge-vector-db.routes'
import { AIKnowledgeVectorDbDetailsComponent } from './pages/ai-knowledge-vector-db-details/ai-knowledge-vector-db-details.component'
import { AIKnowledgeVectorDbDetailsEffects } from './pages/ai-knowledge-vector-db-details/ai-knowledge-vector-db-details.effects'
import { AIKnowledgeVectorDbSearchComponent } from './pages/ai-knowledge-vector-db-search/ai-knowledge-vector-db-search.component'
import { AIKnowledgeVectorDbSearchEffects } from './pages/ai-knowledge-vector-db-search/ai-knowledge-vector-db-search.effects'
import { AIKnowledgeVectorDbCreateUpdateComponent } from './pages/ai-knowledge-vector-db-search/dialogs/ai-knowledge-vector-db-create-update/ai-knowledge-vector-db-create-update.component'

@NgModule({
  providers: [providePortalDialogService()],
  declarations: [
    AIKnowledgeVectorDbCreateUpdateComponent,
    AIKnowledgeVectorDbDetailsComponent,
    AIKnowledgeVectorDbSearchComponent
  ],
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
  imports: [
    CommonModule,
    SharedModule,
    LetDirective,
    PortalCoreModule.forMicroFrontend(),
    RouterModule.forChild(addInitializeModuleGuard(routes)),
    FormsModule,
    ReactiveFormsModule,
    CalendarModule,
    StoreModule.forFeature(AIKnowledgeVectorDbFeature),
    EffectsModule.forFeature([AIKnowledgeVectorDbDetailsEffects, AIKnowledgeVectorDbSearchEffects]),
    TranslateModule
  ]
})
export class AIKnowledgeVectorDbModule {}