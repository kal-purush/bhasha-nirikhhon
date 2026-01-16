import { InMemoryStore } from "./InMemoryStore";

describe("InMemoryStore", () => {
    const userId = "userId";
    const streamId = "streamId";

    test("getting streams for non saved user returns empty set", () => {
        const store = new InMemoryStore();

        expect(store.getStreams(userId)).toEqual(new Set());
    });

    test("can add and retrieve streams for a user", () => {
        const store = new InMemoryStore();

        expect(store.getStreams(userId)).toEqual(new Set());

        store.addStream(userId, streamId);

        expect(store.getStreams(userId)).toEqual(new Set([streamId]));
    });

    test("adding a stream is idempotent", () => {
        const store = new InMemoryStore();

        expect(store.getStreams(userId)).toEqual(new Set());

        store.addStream(userId, streamId);
        store.addStream(userId, streamId);

        expect(store.getStreams(userId)).toEqual(new Set([streamId]));
    });
});