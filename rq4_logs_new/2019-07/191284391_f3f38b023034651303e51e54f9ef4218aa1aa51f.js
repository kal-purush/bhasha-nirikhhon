import { expect, should, assert } from 'chai';
import sinon from 'sinon';

import Assembler from '../../src/assemblers/assembler';
import {registerUser} from '../../src/controllers/auth/registerController';
import User from '../../src/models/User';

describe('Assembler class', () => {
	describe('userRegistrationReqAssembler method', () => {
		let reqObj = {
			firstName: 'samuel',
			lastName: 'grennan',
			email: 'sam.grennan@hotmail.com',
			password: 'hello123',
			role: 'researcher',
		}
		it('should return an assembled user object with additional properties', async () => {
			const result = await Assembler.userRegistrationReqAssembler(reqObj);
			// console.log(result)
			expect(result).to.have.property('firstName');
			expect(result).to.have.property('lastName');
			expect(result).to.have.property('email');
			expect(result).to.have.property('pw');
			expect(result).to.have.property('role');
			expect(result).to.have.property('id');
            expect(result).to.have.property('createdAt');
            
			// TODO
		})
		it('should return a non empty object', async () => {
			const result = await Assembler.userRegistrationReqAssembler(reqObj);
			expect(result).to.be.an('object');
			assert.isNotNull(result);
		})
		it('should include input values', async () => {
			const result = await Assembler.userRegistrationReqAssembler(reqObj);
			const f = {
				firstName: result.firstName,
				lastName: result.lastName,
				email: result.email,
				pw: result.pw,
				role: result.role,
			}
			assert.include(result, f);
		})
    })
    
    describe('userRegistrationResAssembler method', () => {
        let now = new Date();
        let clock = sinon.useFakeTimers(now.getTime());

        let input = {
            _id: '_id',
            id: 'id',
            firstName: 'Tracey',
            lastName: 'Truong',
            email: 'tracey@hotmail.com',
            pw: 'password',
            role: 'investor',
            createdAt: clock
        }

        let output = {
            id: 'id',
            name: 'Tracey Truong',
            email: 'tracey@hotmail.com',
            role: 'investor'
        }

        it('should return a user response object containing id, name, email, role and properties', async() => {
            const result = await Assembler.userRegistrationResAssembler(input);

            expect(result).includes(output);

            clock.restore();
        })


    })
})