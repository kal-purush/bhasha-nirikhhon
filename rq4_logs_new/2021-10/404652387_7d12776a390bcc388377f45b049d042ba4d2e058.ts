import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MesChatsComponent } from './mes-chats.component';

describe('MesChatsComponent', () => {
  let component: MesChatsComponent;
  let fixture: ComponentFixture<MesChatsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ MesChatsComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MesChatsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});