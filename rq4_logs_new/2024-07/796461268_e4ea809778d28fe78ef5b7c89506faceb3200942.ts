import { NextRequest } from "next/server";
import { GET } from "@/app/api/documents/route";
import { auth } from "@clerk/nextjs/server";
import path from "path";
import fs from "fs";

jest.mock("@clerk/nextjs/server", () => ({
  auth: jest.fn(),
}));

describe("/api/documents", () => {
  it("should return a 200 status and the correct message for authenticated users", async () => {
    (auth as jest.Mock).mockReturnValue({
      userId: "user_2i09vFjqeHy5KIfCl6brDwJeOmy",
    });

    const req = new NextRequest(
      "http://localhost:3000/api/documents?id=user_2i09vFjqeHy5KIfCl6brDwJeOmy&skip=0&take=10",
      {
        method: "GET",
        headers: { Authorization: "Bearer valid-token" },
      }
    );

    const res = await GET(req);
    expect(res.status).toBe(200);

    const json = await res.json();
    expect(json.message).toEqual("List of documents");
  });

  it("should return a 400 status when one or more parameters are missing", async () => {
    (auth as jest.Mock).mockReturnValue({
      userId: "user_2i09vFjqeHy5KIfCl6brDwJeOmy",
    });

    const req = new NextRequest("http://localhost:3000/api/documents", {
      method: "GET",
      headers: { Authorization: "Bearer valid-token" },
    });

    const res = await GET(req);
    expect(res.status).toBe(400);

    const json = await res.json();
    expect(["Please provide an id"]).toContain(json.message);
  });
});