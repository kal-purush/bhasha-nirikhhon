import dayjs from "dayjs"
import { createUserFlow } from "../test/userFlows/users.js"
import { createEventFlow } from "../test/userFlows/events.js"
import { callGetAttendees, callJoinEvent } from "../test/apiCallers/events.js"

describe("Join the event by id tests", () => {
  const eventLocation = { latitude: 50, longitude: 50 }

  it("should return 200 after loading the attendees list", async () => {
    const { attendeeToken, eventIds } = await createEventFlow([
      {
        ...eventLocation,
        startTimestamp: dayjs().add(12, "hour").toDate(),
        endTimestamp: dayjs().add(1, "year").toDate()
      }
    ])

    const { token: attendeeToken2 } = await createUserFlow()
    await callJoinEvent(attendeeToken2, eventIds[0])

    const resp = await callGetAttendees(
      attendeeToken,
      eventIds[0],
      "userID_1234_joindate_2022-01-11",
      2
    )

    expect(resp).toMatchObject({
      status: 200,
      body: {
        // TODO
      }
    })
  })
})