import {
  ChangeDetectionStrategy,
  Component,
  CUSTOM_ELEMENTS_SCHEMA,
  inject,
  Input,
  PLATFORM_ID,
} from '@angular/core';
import { CommonModule, DOCUMENT, isPlatformBrowser } from '@angular/common';
import { DocTabPanelComponent } from '../doc-tab-panel/doc-tab-panel.component';
import { defineCustomElements as tabPanelElements } from '@ipedis/tab-panel/loader';
import { ModalComponent } from '../../modal/modal.component';

@Component({
  selector: 'app-tab-panel1',
  standalone: true,
  imports: [CommonModule, DocTabPanelComponent, ModalComponent],
  templateUrl: './tab-panel1.component.html',
  styleUrl: './tab-panel1.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
})
export class TabPanel1Component {
  modalTitle = 'Login 1';
  isModalVisible = false;
  @Input() currentView: 'preview' | 'code' | 'doc' = 'preview';

  handleCloseModal() {
    this.isModalVisible = false;
  }
  openModal() {
    this.isModalVisible = true;
  }
  constructor() {
    if (isPlatformBrowser(inject(PLATFORM_ID)) && tabPanelElements) {
      tabPanelElements(inject(DOCUMENT).defaultView as Window);
    }
  }
}