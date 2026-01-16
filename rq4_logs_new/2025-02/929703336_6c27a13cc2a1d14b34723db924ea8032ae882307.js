// app/api/admin/participants/route.js
import { MongoClient } from "mongodb";

const uri = process.env.MONGODB_URI;

export async function GET() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db("ai-workshop");
    const participants = await db.collection("participants").find({}).toArray();

    // Convert each document into a plain object with computed department.
    const data = participants.map((p) => {
      let department = "AI&ML"; // default
      const regex = /4MW\d+(AD|AI|EC|CS)/i;
      const match = p.usn.match(regex);
      if (match) {
        const code = match[1].toUpperCase();
        if (code === "AD") department = "AI&DS";
        else if (code === "EC") department = "E&C";
        else if (code === "CS") department = "CSE";
      }
      return {
        _id: p._id.toString(), // Convert ObjectId to string
        name: p.name,
        email: p.email,
        usn: p.usn,
        year: p.year,
        phone: p.phone,
        department,
      };
    });

    return new Response(JSON.stringify({ participants: data }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (error) {
    console.error(error);
    return new Response(JSON.stringify({ error: "Error fetching data" }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  } finally {
    await client.close();
  }
}