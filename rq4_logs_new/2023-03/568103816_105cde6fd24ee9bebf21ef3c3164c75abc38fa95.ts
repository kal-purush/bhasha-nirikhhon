export function getEmailTemplate(data: {
    hostFirstName: string;
    eventTitle: string;
    guestName: string;
    guestId: string;
    type: 'join' | 'accepted';
    eventId?: string;
}) {
    if (data.type === 'join') {
        return {
            subject: `${data.guestName} wants to join`,
            text: `${data.guestName} wants to join`,
            html: `<div>Hi ${
                data.hostFirstName
            },</div><p>Somebody wants to join your event <strong>${
                data.eventTitle
            }</strong>!<br>
        Check out your possible guest <a href=${
            'https://mmp3.vercel.app/profile/' + data.guestId
        }>${data.guestName}</a> and answer the request to join.</p>`,
        };
    }
    if (data.type === 'accepted') {
        return {
            subject: `${data.hostFirstName} accepts join request`,
            text: `${data.hostFirstName} accepts join request`,
            html: `<div>Hi ${data.guestName},<br></div><p>${
                data.hostFirstName
            }  just accepted your join request for <strong>${
                data.eventTitle
            }</strong>!<br>
            Check out the event for all information that will be important for you to know <a href=${
                'https://mmp3.vercel.app/events/' + data.eventId
            }>here</a>.<br><br>Enjoy your time at the event, make new friends and have fun!
        `,
        };
    }
}