/* Angular modules */
import { CommonModule } from '@angular/common';
import { forwardRef } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { NG_VALUE_ACCESSOR, NG_VALIDATORS } from '@angular/forms';

/* Third-party modules */
import { MaskedInputDirective } from 'angular2-text-mask';

/* Own modules */
import { MatDateFormatComponent } from './md-date-format.component';
import { DateFormatModel } from '../../model/date-format.model';
import { DateFormatModelMock } from '../../mock/date-format.model.mock';
import { DateFormatValidator } from '../../validator/date-format.validator';
import { DateFormatValidatorMock } from '../../mock/date-format.validator.mock';

describe('MatDateFormatComponent', () => {
    let comp:    MatDateFormatComponent;
    let fixture: ComponentFixture<MatDateFormatComponent>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports:      [ CommonModule, FormsModule ],
            declarations: [ MatDateFormatComponent, MaskedInputDirective ],
            providers: [
                { provide: DateFormatModel, useClass: DateFormatModelMock },
                { provide: DateFormatValidator, useClass: DateFormatValidatorMock },
                { provide: NG_VALUE_ACCESSOR, useExisting: forwardRef(() => MatDateFormatComponent), multi: true },
                { provide: NG_VALIDATORS, useExisting: forwardRef(() => MatDateFormatComponent), multi: true }
            ]
        });

        fixture = TestBed.createComponent(MatDateFormatComponent);
        comp = fixture.componentInstance;
        comp.minDate = '01-01-2016';
        comp.maxDate = '01-01-2017';
    });

    it('should create component', () => expect(comp).toBeDefined() );

    describe('entering invalid values', () => {
        const expectedEmptyValue = '';
        let resultFromInvalidValue;

        it('blank spaces', () => {
            comp.date = '       ';
            resultFromInvalidValue = comp.date;
            expect(resultFromInvalidValue).toEqual(expectedEmptyValue);
        });

        it('undefined', () => {
            comp.date = undefined;
            resultFromInvalidValue = comp.date;
            expect(resultFromInvalidValue).toEqual(expectedEmptyValue);
        });

        it('null', () => {
            comp.date = null;
            resultFromInvalidValue = comp.date;
            expect(resultFromInvalidValue).toEqual(expectedEmptyValue);
        });

        it('textual string', () => {
            comp.date = 'this is an invalid value for date';
            resultFromInvalidValue = comp.date;
            expect(resultFromInvalidValue).toEqual(expectedEmptyValue);
        });
    });

    it('should valid american date being applied', () => {
        const expectedValue = '01-16-2017';
        const emptyValue = '';

        comp.date = '01-16-2017';
        const result = comp.date;
        expect(result).toEqual(expectedValue);
        expect(result).not.toEqual(emptyValue);
    });

    it('should valid brazilian date being applied', () => {
        const expectedValue = '16/01/2017';
        const emptyValue = '';

        comp.date = '16/01/2017';
        const result = comp.date;
        expect(result).toEqual(expectedValue);
        expect(result).not.toEqual(emptyValue);
    });

    it('should key events accepting numbers', () => {
        let e: any;

        e = {
            key: '4'
        };

        expect(comp.keyDownEvent(e)).toBe(false);
        comp.keyUpEvent(e);

        e = {
            key: '1'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '1_/__/____';
        comp.keyUpEvent(e);

        expect(comp._date).toEqual('1_/__/____');

        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '11/__/____';
        comp.keyUpEvent(e);

        expect(comp._date).toEqual('11/__/____');

        e = {
            key: '2'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '11/__/____';
        comp.keyUpEvent(e);

        expect(comp._date).toEqual('11/__/____');
    });

    it('should key events ignoring not numbers keys', () => {
        let e: any;

        e = { key: 'a' };
        expect(comp.keyDownEvent(e)).toBe(undefined);

        e = { key: 'A' };
        expect(comp.keyDownEvent(e)).toBe(undefined);

        e = { key: '&' };
        expect(comp.keyDownEvent(e)).toBe(undefined);
    });

    it('should not accepting dates less than #minDate=01-01-2016', () => {
        let e: any;

        expect(comp.minDate).toBe('01-01-2016');

        e = {
            key: '1'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '1_/__/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('1_/__/____');

        e = {
            key: '2'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/__/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/__/____');

        e = {
            key: '0'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/0_/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/0_/____');

        e = {
            key: '9'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/____');

        e = {
            key: '1'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/____');

        e = {
            key: '2'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/2___';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/2___');

        e = {
            key: '0'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/20__';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/20__');

        e = {
            key: '0'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/20__';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/20__');

        e = {
            key: '1'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/201_';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/201_');

        e = {
            key: '5'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/201_';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/201_');

        e = {
            key: '6'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/2016';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/2016');
    });

    it('should accepting dates between #minDate=01-01-2016 and #maxDate=01-01-2017', () => {
        let e: any;

        expect(comp.minDate).toBe('01-01-2016');
        expect(comp.maxDate).toBe('01-01-2017');

        e = {
            key: '1'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '1_/__/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('1_/__/____');

        e = {
            key: '2'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/__/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/__/____');

        e = {
            key: '0'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/0_/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/0_/____');

        e = {
            key: '9'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/____');

        e = {
            key: '1'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/____');

        e = {
            key: '3'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/____');

        e = {
            key: '2'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/2___';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/2___');

        e = {
            key: '1'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/2___';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/2___');

        e = {
            key: '0'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/20__';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/20__');

        e = {
            key: '0'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/20__';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/20__');

        e = {
            key: '2'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/20__';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/20__');

        e = {
            key: '1'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/201_';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/201_');

        e = {
            key: '5'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/201_';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/201_');

        e = {
            key: '7'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/201_';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/201_');

        e = {
            key: '6'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/2016';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/2016');
    });

    it('should not accepting dates more than #maxDate=01-01-2017', () => {
        let e: any;

        expect(comp.maxDate).toBe('01-01-2017');

        e = {
            key: '1'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '1_/__/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('1_/__/____');

        e = {
            key: '2'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/__/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/__/____');

        e = {
            key: '0'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/0_/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/0_/____');

        e = {
            key: '9'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/____');

        e = {
            key: '3'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/____';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/____');

        e = {
            key: '2'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/2___';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/2___');

        e = {
            key: '1'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/2___';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/2___');

        e = {
            key: '0'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/20__';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/20__');

        e = {
            key: '2'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/20__';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/20__');

        e = {
            key: '1'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/201_';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/201_');

        e = {
            key: '7'
        };
        expect(comp.keyDownEvent(e)).toBe(false);
        comp._date = '12/09/201_';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/201_');

        e = {
            key: '6'
        };
        expect(comp.keyDownEvent(e)).toBe(true);
        comp._date = '12/09/2016';
        comp.keyUpEvent(e);
        expect(comp._date).toEqual('12/09/2016');
    });

});