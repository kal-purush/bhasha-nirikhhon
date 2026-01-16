import { Subscription } from "nats";

import { getConnection, jsonCodec, PublicNatsHandler } from "../services/nats";

export const inventoryPublicHandlers: PublicNatsHandler[] = [
    [
        "POST",
        "inventory",
        async (subscription: Subscription): Promise<void> => {
            for await (const message of subscription) {
                try {
                    const natsConnection = getConnection();

                    const response = await natsConnection.request(
                        "item-store.assign_inventory_item",
                        message.data
                    );

                    message.respond(response.data);
                } catch (err) {
                    message.respond(
                        jsonCodec.encode({
                            error: err.message
                        })
                    );
                }
            }
        }
    ],

    [
        "GET",
        "inventory.*",
        async (subscription: Subscription): Promise<void> => {
            for await (const message of subscription) {
                try {
                    const natsConnection = getConnection();

                    const urlParameter = String(message.subject).split(".")[2];

                    const response = await natsConnection.request(
                        "item-store.get_inventory_item",
                        jsonCodec.encode({
                            inventory_item_id: urlParameter
                        })
                    );

                    message.respond(response.data);
                } catch (err) {
                    message.respond(
                        jsonCodec.encode({
                            error: err.message
                        })
                    );
                }
            }
        }
    ],

    [
        "PUT",
        "inventory.*",
        async (subscription: Subscription): Promise<void> => {
            for await (const message of subscription) {
                try {
                    const natsConnection = getConnection();

                    const urlParameter = String(message.subject).split(".")[2];

                    const data = jsonCodec.decode(message.data);

                    const response = await natsConnection.request(
                        "item-store.update_inventory_item",
                        jsonCodec.encode({
                            inventory_item_id: urlParameter,
                            data
                        })
                    );

                    message.respond(response.data);
                } catch (err) {
                    message.respond(
                        jsonCodec.encode({
                            error: err.message
                        })
                    );
                }
            }
        }
    ]
];