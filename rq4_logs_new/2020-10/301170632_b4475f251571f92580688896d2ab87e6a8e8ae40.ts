import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { BreadcrumbsComponent } from './breadcrumbs.component';


describe( 'BreadcrumbsComponent', () => {

  let breadcrumbsComponent: BreadcrumbsComponent;
  let fixture: ComponentFixture<BreadcrumbsComponent>;

  beforeEach( async () => {
    await TestBed.configureTestingModule( {
      declarations: [ BreadcrumbsComponent ]
    } )
      .compileComponents();
  } );

  beforeEach( () => {
    fixture = TestBed.createComponent( BreadcrumbsComponent );
    breadcrumbsComponent = fixture.componentInstance;
    fixture.detectChanges();
  } );

  it( 'should create breadcrumbs component', () => {
    expect( breadcrumbsComponent ).toBeTruthy();
  } );

  it( 'should initial breadcrumbs', () => {
    breadcrumbsComponent.ngOnInit();
    expect( breadcrumbsComponent.breadcrumbs ).toBeDefined();
  } );

  it( 'should render breadcrumbs', () => {
    const breadcrumbsTitle: string = 'Something';

    breadcrumbsComponent.breadcrumbs = breadcrumbsTitle;
    fixture.detectChanges();
    const breadcrumbsEl: HTMLElement = fixture.debugElement.query( By.css( '.breadcrumbs' ) ).nativeElement;

    expect( breadcrumbsEl.textContent ).toContain( breadcrumbsTitle );
  } );
} );