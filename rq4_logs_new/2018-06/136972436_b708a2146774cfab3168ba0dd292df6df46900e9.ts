import { MessageService } from "./message.service";

describe("MessageService", () => {
    let service: MessageService;

    /**
     * Arrange is inside the before each.
     */
    beforeEach(() => {
        //service = new MessageService();
    });

    it("should have no messages to start", () => {
        service = new MessageService();
        expect(service.messages.length).toBe(0);
    });

    it("should add a message when add is called", () => {
        service = new MessageService();
        service.add("message1");
        expect(service.messages.length).toBe(1);
    });

    it("should clear all added messages when clear is called", () => {
        service = new MessageService();
        service.add("message1");
        service.add("message2");
        service.clear();
        expect(service.messages.length).toBe(0);
    });
});