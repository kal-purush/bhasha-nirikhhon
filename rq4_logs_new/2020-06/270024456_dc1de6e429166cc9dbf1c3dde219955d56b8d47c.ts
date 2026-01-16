import { Request, Response } from 'express';
import knex from '../database/connection';

class ItemsController {
    async index(request: Request, response: Response) {
        const items = await knex('items').select('*');

        const porta = '3333';

        // Pegar IP
        var
            address: String // Local ip address that we're trying to calculate
            ,os = require('os') // Provides a few basic operating-system related utility functions (built-in)
            ,ifaces = os.networkInterfaces(); // Network interfaces

        // Iterate over interfaces ...
        // grava os dados em address
        for (var dev in ifaces) {
            // ... and find the one that matches the criteria
            var iface = ifaces[dev].filter(function(details) {
                return details.family === 'IPv4' && details.internal === false;
            });
            if(iface.length > 0) address = iface[0].address;
        }

        const serializedItems = items.map(item => {
            return {
                id: item.id,
                name: item.title,
                image: item.image,
                image_url: `http://${address}:${porta}/uploads/${item.image}`
            }
        });
    
        return response.json(serializedItems);
    }
}

export default ItemsController;