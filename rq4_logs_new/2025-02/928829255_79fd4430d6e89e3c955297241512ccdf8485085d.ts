import { CommonModule } from '@angular/common'
import { CUSTOM_ELEMENTS_SCHEMA, NgModule } from '@angular/core'
import { FormsModule, ReactiveFormsModule } from '@angular/forms'
import { RouterModule } from '@angular/router'
import { LetDirective } from '@ngrx/component'
import { EffectsModule } from '@ngrx/effects'
import { StoreModule } from '@ngrx/store'
import { TranslateModule } from '@ngx-translate/core'
import { addInitializeModuleGuard, PortalCoreModule, providePortalDialogService } from '@onecx/portal-integration-angular'
import { CalendarModule } from 'primeng/calendar'
import { SharedModule } from '../shared/shared.module'
import { AIKnowledgeDocumentFeature } from './aiknowledge-document.reducers'
import { routes } from './aiknowledge-document.routes'
import { AIKnowledgeDocumentDetailsComponent } from './pages/aiknowledge-document-details/aiknowledge-document-details.component'
import { AIKnowledgeDocumentDetailsEffects } from './pages/aiknowledge-document-details/aiknowledge-document-details.effects'
import { AIKnowledgeDocumentSearchComponent } from './pages/aiknowledge-document-search/aiknowledge-document-search.component'
import { AIKnowledgeDocumentSearchEffects } from './pages/aiknowledge-document-search/aiknowledge-document-search.effects'
import { AIKnowledgeDocumentCreateUpdateComponent } from './pages/aiknowledge-document-search/dialogs/aiknowledge-document-create-update/aiknowledge-document-create-update.component'

@NgModule({
  providers: [providePortalDialogService()],
  declarations: [
    AIKnowledgeDocumentCreateUpdateComponent,
    AIKnowledgeDocumentDetailsComponent,
    AIKnowledgeDocumentSearchComponent
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
    StoreModule.forFeature(AIKnowledgeDocumentFeature),
    EffectsModule.forFeature([AIKnowledgeDocumentDetailsEffects, AIKnowledgeDocumentSearchEffects]),
    TranslateModule
  ]
})
export class AIKnowledgeDocumentModule { }