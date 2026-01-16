import { TestBed } from '@angular/core/testing'
import {APP_BASE_HREF} from '@angular/common';
import { configureTests } from '../../test.configure'

import { RickJamesComponent} from './rick.james.component'
import { RickJamesModule } from './rick.james.module'
import { RouterTestingModule } from '@angular/router/testing'

describe("RickJamesComponent", () => {
    let fixture;
    beforeEach((done) => {
        const configure = (testBed: TestBed) => {
            testBed.configureTestingModule({
                imports: [RickJamesModule, RouterTestingModule],
                providers: []
            })
        }

        configureTests(configure).then((testBed) => {
            fixture = testBed.createComponent(RickJamesComponent)
            fixture.detectChanges()
            done()
        })
    })

    it("should have a passing test", () => {
        expect(fixture).toBeTruthy()
    })
})