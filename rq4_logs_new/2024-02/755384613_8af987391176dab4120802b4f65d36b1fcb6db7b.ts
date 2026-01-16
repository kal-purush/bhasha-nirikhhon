import { updateDoc, doc } from "firebase/firestore";
import { database } from "@/firebase/config";

export async function PUT(
  req: Request,
  { params }: { params: { id: string } },
) {
  try {
    const payload = await req.json();
    const { id } = params;

    await updateDoc(doc(database, "questionnaires", id as string), {
      ...payload,
      updated_at: new Date().toISOString(),
    });
    return Response.json({
      message: "Questionnaire price updated successfully👍",
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