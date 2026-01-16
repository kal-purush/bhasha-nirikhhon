import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PostNotificacionComponent } from './post-notificacion.component';

describe('PostNotificacionComponent', () => {
  let component: PostNotificacionComponent;
  let fixture: ComponentFixture<PostNotificacionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PostNotificacionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PostNotificacionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});