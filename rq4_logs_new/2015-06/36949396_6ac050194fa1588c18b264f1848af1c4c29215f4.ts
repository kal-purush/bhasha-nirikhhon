module Assert {
    /**
     * Assert that a value is `truthy`.
     * @see {@link https://developer.mozilla.org/en-US/docs/Glossary/Truthy}
     */
    export function truthy(value: any): void {
        if (!value) {
            fail("Expected value to be truthy.");
        }
    }

    /**
     * Asserts that a value is `falsy`.
     * @see {@link https://developer.mozilla.org/en-US/docs/Glossary/Falsy}
     */
    export function falsy(value: any): void {
        if (value) {
            fail("Expected value to be falsy.");
        }
    }

    function fail(errorMessage: string): void {
        throw new Error("Assertion error: " + errorMessage);
    }
}