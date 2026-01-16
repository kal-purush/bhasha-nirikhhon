import { CommunicationChannel } from "@/constant";
import Airtable from "airtable";
import { NextResponse } from "next/server";

// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
const apiKey = process.env.AIRTABLE_API_KEY!;
const baseId = "appU5MIM7Yl1VrxEm";
const tableId = "tbl2scUK2gyVCSWtj";
const organizationFieldId = "fldyCZtTyHT8jWLXa";
const integrationFieldId = "fldafGL8jvE3toWk8";
const nameFieldId = "fldX9QMw47XvzSW86";
const communicationChannelFieldId = "fldjxMaR5DEtF29GC";
const contactDetailsFieldId = "fldRByHAhU5Wbg9pF";
const airtableBase = new Airtable({ apiKey }).base(baseId);
export async function POST(request: Request) {
  const body = (await request.json()) as {
    name: string;
    organization: string;
    communicationChannel: CommunicationChannel;
    contactDetails: string;
  };
  const fields = {
    [nameFieldId]: body.name,
    [organizationFieldId]: body.organization,
    [communicationChannelFieldId]: getAirtableCommunicationChannelName(body.communicationChannel),
    [contactDetailsFieldId]: body.contactDetails,
    [integrationFieldId]: "oSnap",
  };

  try {
    const record = await airtableBase(tableId).create(fields);
    const recordId = record.getId();
    console.log(`Successfully created record ${recordId} in table ${tableId}`);
    return NextResponse.json({
      status: 200,
      body: `Successfully created record ${recordId} in table ${tableId}`,
    });
  } catch (error) {
    console.error("Failed to create record in Airtable", error);
    return NextResponse.json({
      status: 500,
      body: `Failed to create record in Airtable`,
    });
  }
}

function getAirtableCommunicationChannelName(channel: CommunicationChannel) {
  if (channel === "other") {
    return "";
  }
  return channel.charAt(0).toUpperCase() + channel.slice(1);
}