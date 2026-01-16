import { creditCardValidator } from './credit-card-validator';
import { AbstractControl, FormBuilder} from '@angular/forms';

describe('credit card number validator', () => {
    let getControl = (valueToTest: string) : AbstractControl => {
        let fb = new FormBuilder();
        let customerForm = fb.group({
            cardNumber: [valueToTest, creditCardValidator()]
        });
        return customerForm.get('cardNumber') as AbstractControl;
    };

    it('card number containing 15 numbers should be valid', () => {
        let control = getControl('123456789012345');
        let isControlValid = control.valid;
        expect(isControlValid).toBeTruthy();
    });

    it('card number containing letters should be invalid', () => {
        let control = getControl('a234567890123456');
        let isControlValid = control.valid;
        expect(isControlValid).toBeFalsy();
    });

    it('card numbers that are emptyshould be invalid', () => {
        let control = getControl('');
        let isControlValid = control && control.valid;
        expect(isControlValid).toBeFalsy();
    });
});

