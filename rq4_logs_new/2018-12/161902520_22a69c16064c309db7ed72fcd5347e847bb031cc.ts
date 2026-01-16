import { CooperProvider } from './../../providers/cooper/cooper';
import { CalculationProvider } from './../../providers/calculation/calculation';
import { TestBed, inject } from "@angular/core/testing";


describe("Calculation Component", () => {
    let calculationProvider, cooperProvider;

	beforeEach(() => TestBed.configureTestingModule({
    providers: [CalculationProvider, CooperProvider]
  }));

  beforeEach(inject([CalculationProvider, CooperProvider], (p, c) => {
    calculationProvider = p;
    cooperProvider = c;
  }));

    it("should create the calculation provider", () => {
        expect(calculationProvider).toBeTruthy();
        expect(calculationProvider instanceof CalculationProvider).toEqual(true);
    );

    // it('doassessment should be defined', () => {
    //     spyOn(calculationProvider, 'doAssessment');

    //     calculationProvider.doAssessment(2500);

    //     expect(personProvider.doAssessment).toHaveBeenCalled();
    //     expect(personProvider.doAssessment).toHaveBeenCalledWith(2500);
    // });

    it('cooper provider should be called', () => {
        calculationProvider.high_score = 10;
        spyOn(cooperProvider, 'assess');

        calculationProvider.doAssessment(20);

        expect(cooperProvider.assess).toHaveBeenCalled();
        expect(cooperProvider.assess).toHaveBeenCalledWith(calculationProvider, 20);
    });
});