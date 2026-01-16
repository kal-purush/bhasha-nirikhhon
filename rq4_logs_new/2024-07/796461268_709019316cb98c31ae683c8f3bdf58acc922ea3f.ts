import { NextRequest } from "next/server";
import { GET, POST, PUT, DELETE } from "@/app/api/chatbox/route";

describe("/api/chatbox", () => {
  it("should return a 200 status and the correct message for authenticated users", async () => {
    const req = new NextRequest(
      "http://localhost:3000/api/chatbox?user_id=user_2i09vFjqeHy5KIfCl6brDwJeOmy",
      {
        method: "GET",
        headers: { Authorization: "Bearer valid-token" },
      }
    );

    const res = await GET(req);
    expect(res.status).toBe(200);

    const json = await res.json();
    expect(json.message).toEqual("Record fetched successfully");
  });

  it("should return a 400 status when userId parameter is missing", async () => {
    const req = new NextRequest("http://localhost:3000/api/chatbox", {
      method: "GET",
      headers: { Authorization: "Bearer valid-token" },
    });

    const res = await GET(req);
    expect(res.status).toBe(400);

    const json = await res.json();
    expect(json.message).toEqual("Please provide an user_id");
  });

  it("should return a 200 status and the correct message for authenticated users", async () => {
    const formData = new FormData();
    formData.append("userId", "user_2i09vFjqeHy5KIfCl6brDwJeOmy");
    formData.append("name", "New Chatbox");

    const req = new NextRequest("http://localhost:3000/api/chatbox", {
      method: "POST",
      body: formData,
      headers: { Authorization: "Bearer valid-token" },
    });

    const res = await POST(req);
    expect(res.status).toBe(200);

    const json = await res.json();
    expect(json.message).toEqual("Record created successfully");
  });

  it("should return a 400 status when userId parameter is missing", async () => {
    const formData = new FormData();
    const req = new NextRequest("http://localhost:3000/api/chatbox", {
      method: "POST",
      body: formData,
      headers: { Authorization: "Bearer valid-token" },
    });

    const res = await POST(req);
    expect(res.status).toBe(400);

    const json = await res.json();
    expect(json.message).toEqual("Please fill in all fields");
  });

  it("should return a 200 status and the correct message for authenticated users", async () => {
    const formData = new FormData();
    formData.append("id", "1");
    formData.append("name", "New Chatbox");

    const req = new NextRequest("http://localhost:3000/api/chatbox", {
      method: "PUT",
      body: formData,
      headers: { Authorization: "Bearer valid-token" },
    });

    const res = await PUT(req);
    expect(res.status).toBe(200);

    const json = await res.json();
    expect(json.message).toEqual("Record updated successfully");
  });

  it("should return a 400 status when userId parameter is missing", async () => {
    const formData = new FormData();
    const req = new NextRequest("http://localhost:3000/api/chatbox", {
      method: "PUT",
      body: formData,
      headers: { Authorization: "Bearer valid-token" },
    });

    const res = await PUT(req);
    expect(res.status).toBe(400);

    const json = await res.json();
    expect(json.message).toEqual("Please fill in all fields");
  });

  it("should return a 200 status and the correct message for authenticated users", async () => {
    const formData = new FormData();
    formData.append("id", "1");
    formData.append("name", "New Chatbox");

    const req = new NextRequest("http://localhost:3000/api/chatbox?id=1", {
      method: "DELETE",
      body: formData,
      headers: { Authorization: "Bearer valid-token" },
    });

    const res = await DELETE(req);
    expect(res.status).toBe(200);

    const json = await res.json();
    expect(json.message).toEqual("Record deleted successfully");
  });

  it("should return a 400 status when userId parameter is missing", async () => {
    const formData = new FormData();
    const req = new NextRequest("http://localhost:3000/api/chatbox", {
      method: "DELETE",
      body: formData,
      headers: { Authorization: "Bearer valid-token" },
    });

    const res = await DELETE(req);
    expect(res.status).toBe(400);

    const json = await res.json();
    expect(json.message).toEqual("Please provide an id");
  });
});