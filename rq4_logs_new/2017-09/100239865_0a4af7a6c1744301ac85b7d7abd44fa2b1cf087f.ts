import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AdminComponent } from './admin.component';
import { AdminCommentComponent } from './admin-comment/admin-comment.component';
import { AdminCreateArticleComponent } from './admin-create-article/admin-create-article.component';
import { AdminHomeComponent } from './admin-home/admin-home.component';
import { AdminManageArticleComponent } from './admin-manage-article/admin-manage-article.component';
import { AdminMessageComponent } from './admin-message/admin-message.component';
import { AdminSettingComponent } from './admin-setting/admin-setting.component';
import { AdminUserComponent } from './admin-user/admin-user.component';

import { NgZorroAntdModule } from 'ng-zorro-antd';

// import { FroalaEditorModule, FroalaViewModule } from 'angular-froala-wysiwyg';

import { routing } from './admin.routing';

@NgModule({
    declarations: [
        AdminComponent,
        AdminCommentComponent,
        AdminCreateArticleComponent,
        AdminHomeComponent,
        AdminManageArticleComponent,
        AdminMessageComponent,
        AdminSettingComponent,
        AdminUserComponent,
    ],
    imports: [
        CommonModule,
        NgZorroAntdModule,
        FormsModule,
        routing,
        // FroalaEditorModule.forRoot(),
        // FroalaViewModule.forRoot(),
    ],
})
export class AdminModule { }