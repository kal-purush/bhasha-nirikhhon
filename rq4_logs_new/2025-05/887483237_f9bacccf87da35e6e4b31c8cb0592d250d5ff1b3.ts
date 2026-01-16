import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { ToastComponent } from './toast.component';
import { By } from '@angular/platform-browser';

describe('ToastComponent', () => {
  let component: ToastComponent;
  let fixture: ComponentFixture<ToastComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [ToastComponent]
    });
    fixture = TestBed.createComponent(ToastComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('deve ser criado', () => {
    expect(component).toBeTruthy();
  });

  it('deve exibir e esconder a mensagem após 3 segundos', fakeAsync(() => {
    component.show('Mensagem de teste');
    fixture.detectChanges();

    // Deve estar visível após chamada
    let toastEl = fixture.debugElement.query(By.css('.toast'));
    expect(component.visible).toBeTrue();
    expect(toastEl).toBeTruthy();
    expect(toastEl.nativeElement.textContent).toContain('Mensagem de teste');

    tick(3000); // Espera 3 segundos
    fixture.detectChanges();

    // Deve ter desaparecido
    expect(component.visible).toBeFalse();
    toastEl = fixture.debugElement.query(By.css('.toast'));
    expect(toastEl).toBeFalsy();
  }));
});