import { GasMeter } from "@mui/icons-material";
import { useSearchParams } from "next/navigation";
import { log } from "console";
import { dbRequest } from "../../config/dbRequest";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const Id = searchParams.get("Id") || "";
  console.log(Id);
  if (Id) {
    const { documents } = await dbRequest("flightData", "find", {
      filter: {
        _id: { $oid: Id },
      },
    });
    return Response.json(documents);
  } else {
    const { documents } = await dbRequest("flightData", "find");
    return Response.json(documents);
  }
}