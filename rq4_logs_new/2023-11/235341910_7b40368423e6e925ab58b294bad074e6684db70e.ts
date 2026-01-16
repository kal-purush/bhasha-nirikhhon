declare global {
    interface Window {
        enableMockApi?: (enable: boolean) => void;
    }
}

export async function enableApiMocking() {
    if (process.env.NODE_ENV !== "development") {
        return;
    }

    // Add a function to window that can enable api mocking
    const enableMockApi = (enable: boolean) => {
        localStorage.setItem("enableMockApi", enable ? "true" : "false");
        window.location.reload();
    };
    window.enableMockApi = enableMockApi;

    if (localStorage.getItem("enableMockApi") === "true") {
        // Start the mock service worker
        try {
            const { worker } = await import("./browser");
            await worker.start();
        } catch (error) {
            console.error("Failed to start mock service worker", error);
        }
    } else {
        // Set the local storage value to false by default
        localStorage.setItem("enableMockApi", "false");
    }
}