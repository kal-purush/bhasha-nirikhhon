import { GasMeter } from "@mui/icons-material";
import { dbRequest } from "../../config/dbRequest";
import { useSearchParams } from "next/navigation";

export async function DELETE(
  request: Request,
  { params }: { params: { id: string } }
) {
  try {
    await dbRequest("flightData", "deleteOne", {
      filter: {
        _id: { $oid: params.id },
      },
    });

    return Response.json({ message: "Success" });
  } catch (error) {
    console.log(error);
    throw new Error("aldaa");
  }
}

export async function PUT(
  request: Request,
  { params }: { params: { id: string } }
) {
  try {
    await dbRequest("flightData", "updateOne", {
      filter: {
        _id: { $oid: params.id },
      },
      update: {
        $set: {
          name: "complete",
        },
      },
    });

    return Response.json({ message: "Success" });
  } catch (error) {
    console.log(error);
    throw new Error("aldaa");
  }
}