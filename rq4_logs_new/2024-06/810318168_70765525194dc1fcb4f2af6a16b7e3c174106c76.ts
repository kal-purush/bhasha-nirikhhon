import { db } from "@/lib/db";
import { NextResponse } from "next/server";





export async function DELETE(req:Request,{ params }: { params: { actionId: string } }) {
    const { actionId } = params;
    if (!actionId) {
      return Response.json({ error: "ClimateAction ID is required." }, { status: 400 });
    }
  
    try {
      await db.climateAction.delete({
        where: { id:actionId }
      });
  
      return NextResponse.json({ message: "ClimateAction deleted successfully." });
    } catch (error) {
      return NextResponse.json({ error: "An error occurred while deleting the ClimateAction." }, { status: 500 });
    }
  }


  export async function PATCH(req:Request,{ params }: { params: { actionId: string } }) {
    const {  title, startYear, emissionsReduction, cost } = await req.json();
    const { actionId } = params;

    if (!actionId) {
      return NextResponse.json({ error: "ClimateAction ID is required." }, { status: 400 });
    }
  
    try {
      const updatedClimateAction = await db.climateAction.update({
        where: { id:actionId },
        data: {
          startYear,
          emissionsReduction,
          actionCost:cost,
          title
        }
      });
  
      return Response.json(updatedClimateAction);
    } catch (error) {
      console.error(error);
      return Response.json({ error: "An error occurred while updating the ClimateAction." }, { status: 500 });
    }
  }