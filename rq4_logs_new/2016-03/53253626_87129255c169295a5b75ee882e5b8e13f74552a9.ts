'use strict';

import * as entity from '../../../src/server/entity/ticket';

console.log('start');

describe('ticket', async () => {

    beforeEach(async (done) => {
        await entity.TicketModel.sync({ force: true });

        await entity.TicketModel.create(
            entity.Ticket.of({ title: 'title', desc: 'desc' }
            ));

        done();
    });

    it('findAll', async (done) => {

        let tickets = await entity.TicketModel.findAll();
        expect(tickets[0].validate()).toBe(true);
        expect(tickets.length).toBe(1);
        done();

    });

});

// describe('ticket', async () => {

//     await entity.TicketModel.sync({ force: true });

//     let ticket = await entity.TicketModel.create(entity.Ticket.of('title', 'desc'));

//     it('findAll', async () => {

//         let tickets = await entity.TicketModel.findAll();
//         console.log(tickets);
//         expect(1).actual(tickets.length);

//     });

// });