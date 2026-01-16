import { TestBed, ComponentFixture } from '@angular/core/testing';

import { TerminalComponent } from './terminal.component';


function makeKeyPressEvent(keyName, keyCode, charCode) {
    const event = new KeyboardEvent('keypress');
    Object.defineProperties(event, {
        charCode: {value: charCode},
        keyCode: {value: keyCode},
        keyIdentifier: {value: keyName},
        which: {value: keyCode}
    });

    return event;
}

describe('Terminal Component', () => {
    let terminal: any;
    let fixture: ComponentFixture<TerminalComponent>;

    beforeEach(done => {
        TestBed.configureTestingModule({
            imports: [
            ],
            declarations: [TerminalComponent],
            providers: [
            ]
        }).compileComponents().then(() => {
            fixture = TestBed.createComponent(TerminalComponent);
            fixture.detectChanges();
            terminal = fixture.componentInstance.terminal;
            done();
        });
    });

    afterEach(() => {
        fixture.componentInstance.ngOnDestroy();
    });

    function focusTest(targetCls) {
        const element = fixture.nativeElement.querySelectorAll('.terminalMenu');
        const cls = element[0].getAttribute('class');
        expect(cls.indexOf(targetCls)).toBeTruthy();
        return element;
    }

    it('terminal state changes from blur to focus', done => {
        focusTest('terminalMenuFocus');

        // Change to blur
        fixture.componentInstance.terminal.blur();
        fixture.detectChanges();
        const element = focusTest('terminalMenuBLur');

        // Change back to focus
        element[0].click();
        fixture.detectChanges();
        focusTest('terminalMenuFocus');

        done();
    });

    function keyboardTest(event) {
        terminal.element.focus();
        terminal.element.dispatchEvent(event);
        fixture.detectChanges();
    }

    function validateData(validatorFn, done) {
        fixture.componentInstance.terminal.on('data', (data) => {
            validatorFn(data);
            fixture.componentInstance.terminal.off('data', () => {});
            done();
        });
    }

    it('User types data', done => {
        validateData(data => expect(data).toEqual('a'), done);

        const event = new KeyboardEvent('keypress', {
            charCode: 97,
            code: 'KeyA',
            key: 'a'
        } as KeyboardEventInit);

        keyboardTest(event);
    });

    it('User presses enter', done => {
        const code = 13;
        validateData(data => expect(data.charCodeAt(0)).toEqual(code), done);
        const event = makeKeyPressEvent('Enter', code, code);
        keyboardTest(event);
    });

    it('User deletes data', done => {
        const code = 8;
        validateData(data => expect(data.charCodeAt(0)).toEqual(code), done);
        const event = makeKeyPressEvent('Enter', code, code);
        keyboardTest(event);
    });
});