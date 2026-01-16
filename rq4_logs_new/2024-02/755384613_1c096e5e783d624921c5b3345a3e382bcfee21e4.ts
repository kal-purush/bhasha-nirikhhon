import { getDoc, updateDoc, doc } from "firebase/firestore";
import { database } from "@/firebase/config";

export async function GET(
  req: Request,
  { params }: { params: { id: string } }
) {
  try {
    const { id } = params;

    const questionnaireData = await getDoc(
      doc(database, "orders", id as string)
    );
    return Response.json(questionnaireData.data());
  } catch (error: any) {
    return new Response(error, {
      status: 400,
    });
  }
}

export async function PUT(
  req: Request,
  { params }: { params: { id: string } }
) {
  try {
    const payload = await req.json();
    const { id } = params;

    await updateDoc(doc(database, "orders", id as string), {
      ...payload,
      updated_at: new Date().toISOString(),
    });
    return Response.json({
      message: "Order updated successfully👍",
      data: {
        id,
        ...payload,
      }, // Return the unique key generated for the new task
    });
  } catch (error: any) {
    return new Response(error, {
      status: 400,
    });
  }
}