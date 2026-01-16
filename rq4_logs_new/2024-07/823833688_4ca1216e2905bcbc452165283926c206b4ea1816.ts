import { expect, vi, describe, it, afterEach } from "vitest";
import * as NextHeaders from "next/headers";
import jwt from "jsonwebtoken";
import { POST } from "./route";

vi.mock("next/headers");

describe("API Restricted Route", () => {
    afterEach(() => {
        vi.resetAllMocks();
    });
    const spyHeaders = vi.spyOn(NextHeaders, "headers");
    const spyVerify = vi.spyOn(jwt, "verify");

    it("redirects when not signed in", async () => {
        spyHeaders.mockReturnValue({
            get: (name: string) => null,
        } as never);
        spyVerify.mockReturnValue("mock-decoded-string" as never);
        const resp = await POST();
        expect(resp.status).toBe(200);
    });
});