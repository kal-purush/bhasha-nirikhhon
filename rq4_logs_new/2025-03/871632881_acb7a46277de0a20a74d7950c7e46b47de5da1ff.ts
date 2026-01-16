import { NextRequest, NextResponse } from 'next/server';
import { connect } from '@/lib/dbConfig';
import { JiraTicketsDetail } from '@/lib/model/jiraTicketDetailSchema';
import { getTicketDetailsFromJira } from '@/services/jiraService';

export async function PATCH(req: NextRequest, { params }: { params: { ticketKey: string } }) {
  try {
    await connect();
    const { ticketKey } = params;
    const body = await req.json();
    const { category, newBoard } = body;
    
    if (!category) {
      return NextResponse.json({ message: "Category is required" }, { status: 400 });
    }

    // map incoming category names to schema keys. Adjust these mappings as per  frontend values.
    const categoryMapping: Record<string, string> = {
      "Current Ticket": "currentTickets",
      "New Ticket": "newTickets",
      "QA Ticket": "qaTickets",
      "Hold Ticket": "holdTickets"
    };
    const normalizedCategory = categoryMapping[category] || category;

    // Determine which board to fetch: use newBoard if provided, else the ticketKey.
    const boardToFetch = newBoard || ticketKey;

    // Fetch fresh Jira data using the batch API (passing an array with one element).
    const freshDataArr = await getTicketDetailsFromJira([boardToFetch]);
    
    if (!freshDataArr || freshDataArr.length === 0) {
      return NextResponse.json({ message: "Failed to fetch fresh Jira data" }, { status: 404 });
    }
    const freshData = freshDataArr[0];

    // Prepare the updated ticket data based on fresh Jira response.
    const updatedTicketData = {
      key: freshData.key,
      summary: freshData.summary,
      status: freshData.status.name,
      priority: freshData.priority,
      assignee: freshData.assignee,
      assigneeImage: freshData.assigneeImage,
      reported: freshData.reported,
      reportedImage: freshData.reportedImage,
      ticketLink: freshData.link,
      createdAt: freshData.createdAt || freshData.created,
    };

    // Update the specific ticket in the JiraTicketsDetail document.   
    const updateResult = await JiraTicketsDetail.updateOne(
        { [`jiraTicketDetail.${normalizedCategory}`]: { $elemMatch: { key: ticketKey } } },
        { $set: { [`jiraTicketDetail.${normalizedCategory}.$[elem]`]: updatedTicketData, updatedAt: new Date() } },
        { arrayFilters: [{ "elem.key": ticketKey }], new: true }
      );

      
    return NextResponse.json(
      { message: "Ticket updated successfully", updateResult,updatedTicketData: updatedTicketData },
      { status: 200 }
    );
  } catch (error: any) {
    console.error("Error in PATCH /api/ticket-detail/[ticketKey]:", error);
    return NextResponse.json(
      { message: "Error updating ticket", error: error.message },
      { status: 500 }
    );
  }
}