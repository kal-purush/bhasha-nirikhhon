import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { UiComponent } from './ui.component';
import { FormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';

describe('ui unit tests:', () => {
  let component: UiComponent;
  let fixture: ComponentFixture<UiComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [UiComponent],
      imports: [FormsModule],
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UiComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('Should call imc method', () => {

    // Arrange
    let result = 0;
    component.altura = 175;
    component.peso = 80;

    // Act
    component.calculos();
    result = component.imcLocal;

    // Assert
    expect(parseFloat(component.imcLocal.toFixed(1))).toBe(26.1);

  });

  it('Should call clasificacion method', () => {

    // Arrange
    let result = 0;
    component.altura = 175;
    component.peso = 80;

    // Act
    component.calculos();
    result = component.clasificacion;

    // Assert
    expect(component.clasificacion).toBe("Sobre peso grado 1");

  });

  it('Should call piH method', () => {

    // Arrange
    let result = 0;
    component.altura = 175;
    component.peso = 80;
    component.hombre = true;
    component.mujer = false;

    // Act
    component.calculos();
    result = component.pi;

    // Assert
    expect(parseFloat(component.pi.toFixed(1))).toBe(68.8);

  });

  it('Should call piM method', () => {

    // Arrange
    let result = 0;
    component.altura = 175;
    component.peso = 80;
    component.hombre = false;
    component.mujer = true;

    // Act
    component.calculos();
    result = component.pi;

    // Assert
    expect(parseFloat(component.pi.toFixed(1))).toBe(65);

  });

  it('Should calculate imc from altura and peso when i click the "Calcular" button', () => {
    // Arrange 
    component.altura = 175;
    component.peso = 80;
    let calculateButton = fixture.debugElement.query(By.css('.boton_personalizado'));

    // Act
    calculateButton.triggerEventHandler('click', null);

    // Assert
    expect(parseFloat(component.imcLocal.toFixed(1))).toBe(26.1);

  });

  it('Should calculate pi from altura when i click the "Calcular" button', () => {
    // Arrange 
    component.altura = 175;
    let calculateButton = fixture.debugElement.query(By.css('.boton_personalizado'));

    // Act
    calculateButton.triggerEventHandler('click', null);

    // Assert
    expect(parseFloat(component.pi.toFixed(1))).toBe(68.8);

  });

  it('Should render IMC in ticket', () => {
    // Arrange
    component.altura = 175;
    component.peso = 80;

    // Act
    component.calculos();
    fixture.detectChanges();
    let de = fixture.debugElement.query(By.css('.imcLocal'));
    let el: HTMLElement = de.nativeElement;

    // Assert
    expect(parseFloat(el.innerText).toFixed(1)).toContain('26.1');

  });

  it('Should render PI in ticket', () => {
    // Arrange
    component.altura = 175;

    // Act
    component.calculos();
    fixture.detectChanges();
    let de = fixture.debugElement.query(By.css('.pi'));
    let el: HTMLElement = de.nativeElement;

    // Assert
    expect(parseFloat(el.innerText).toFixed(1)).toContain('68.8');

  });

  it('Should set edad through ngModel', async () => {
    // Arrange 
    await fixture.whenStable();
    fixture.detectChanges();
    const inputElement = fixture.debugElement.query(By.css('input[name="edad"]')).nativeElement;

    // Act 
    inputElement.value = '19';
    inputElement.dispatchEvent(new Event('input'));
    fixture.detectChanges();

    // Assert 
    expect(component.edad).toEqual('19');
  });

  it('Should set peso through ngModel', async () => {
    // Arrange 
    await fixture.whenStable();
    fixture.detectChanges();
    const inputElement = fixture.debugElement.query(By.css('input[name="peso"]')).nativeElement;

    // Act 
    inputElement.value = '80';
    inputElement.dispatchEvent(new Event('input'));
    fixture.detectChanges();

    // Assert 
    expect(component.peso).toEqual('80');
  });

  it('Should set altura through ngModel', async () => {
    // Arrange 
    await fixture.whenStable();
    fixture.detectChanges();
    const inputElement = fixture.debugElement.query(By.css('input[name="altura"]')).nativeElement;

    // Act 
    inputElement.value = '175';
    inputElement.dispatchEvent(new Event('input'));
    fixture.detectChanges();

    // Assert 
    expect(component.altura).toEqual('175');
  });

});