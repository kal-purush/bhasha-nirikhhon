import Spigot from "./Spigot";

const spigot = new Spigot();
const ids = {
    valid: {
        author: 207301,
        authorUsername: "Th3Shadowbroker",
        resource: 72325,
        update: 309959
    },

    invalid: {
        author: 0,
        authorUsername: "someuserthatdoesnotexist",
        resource: 0,
        update: 0
    }
}

// Resources
describe("Resources", () => {


    // Get a specific resource by its ID.
    test(`Test getResource with valid resource ${ids.valid.resource}`, async () => {
        const resource = await spigot.getResource(ids.valid.resource);
        expect(resource!.id).toBe(ids.valid.resource);
    });

    test(`Test getResource with invalid resource ${ids.invalid.resource}`, async () => {
        const resource = await spigot.getResource(ids.invalid.resource);
        expect(resource).toBe(undefined);
    });


    // List resources.
    test("Test listResources", async () => {
        const resources = await spigot.getResources();
        expect(resources.length).toBeGreaterThan(0);
    });


    // Get resources of an author.
    test(`Test getResourcesByAuthor with valid author ${ids.valid.author}`, async () => {
        const resources = await spigot.getResourcesByAuthor(ids.valid.author);
        expect(resources.length).toBeGreaterThan(0);
    });

    test(`Test getResourcesByAuthor with invalid author ${ids.invalid.author}`, async () => {
        const resources = await spigot.getResourcesByAuthor(ids.invalid.author);
        expect(resources.length).toBe(0);
    });

});

// Resource updates
describe("Updates", () => {

    // Get a specific resource update by its ID.
    test(`Test getResourceUpdate with valid id ${ids.valid.update}`, async () => {
        const update = await spigot.getResourceUpdate(ids.valid.update);
        expect(update!.id).toBe(ids.valid.update);
    });

    test(`Test getResourceUpdate with invalid id ${ids.invalid.update}`, async () => {
        const update = await spigot.getResourceUpdate(ids.invalid.update);
        expect(update).toBe(undefined);
    });


    // Get the updates of a resource.
    test("Test getResourceUpdates", async () => {
        const updates = await spigot.getResourceUpdates(ids.valid.resource);
        expect(updates.length).toBeGreaterThan(0);
    });

});

// Authors
describe("Authors", () => {

    // Get a specific author by their ID.
    test(`Test getAuthor with valid author ${ids.valid.author}`, async () => {
        const author = await spigot.getAuthor(ids.valid.author);
        expect(author!.id).toBe(ids.valid.author);
    });

    test(`Test getAuthor with invalid author ${ids.invalid.author}`, async () => {
        const author = await spigot.getAuthor(ids.invalid.author);
        expect(author).toBe(undefined);
    });

    // Find an author by their username.
    test(`Test findAuthor with valid author ${ids.valid.authorUsername}`, async () => {
        const author = await spigot.findAuthor(ids.valid.authorUsername);
        expect(author!.id).toBe(ids.valid.author);
    });

    test(`Test findAuthor with invalid author ${ids.invalid.authorUsername}`, async () => {
        const author = await spigot.findAuthor(ids.invalid.authorUsername);
        expect(author).toBe(undefined);
    });

});

// Everythin else
describe("Etc", () => {

    // Get a list of all resource categories.
    test("Test listResourceCategories", async () => {
        const categories = await spigot.getResourceCategories();
        expect(categories.length).toBeGreaterThan(0);
    });

});