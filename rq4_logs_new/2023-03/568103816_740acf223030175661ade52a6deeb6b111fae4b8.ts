export function getEmailTemplate(
    hostFirstName: string,
    eventTitle: string,
    guestName: string,
    guestId: string
) {
    return {
        subject: `${guestName} wants to join`,
        text: `${guestName} wants to join`,
        html: `<div>Hi ${hostFirstName},</div><p>Somebody wants to join your event <strong>${eventTitle}</strong>!<br>
        Check out your possible guest <a href=${
            'https://mmp3.vercel.app/profile/' + guestId
        }>${guestName}</a> and answer the request to join.</p>`,
    };
}