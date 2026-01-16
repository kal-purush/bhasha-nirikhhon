import jwt from 'jsonwebtoken'
import sgMail from '@sendgrid/mail'

function generateInvitationLink(companyUrl: string): string {
  const payload = {
    companyUrl,
  }

  const options = {
    expiresIn: process.env.JWT_LIFETIME,
  }

  const token = jwt.sign(payload, process.env.JWT_SECRET, options)

  const invitationLink = `${companyUrl}/invitations/${token}`
  return invitationLink
}

export async function sendInvitationEmail(email: string, invitationLink: string): Promise<void> {
  const msg = {
    to: email,
    from: 'your-email@example.com',
    subject: 'Invitation to join our company',
    text: `You have been invited to join our company. Please click the following link to accept the invitation and access our platform: ${invitationLink}`,
    html: `<p>You have been invited to join our company. Please click the following link to accept the invitation and access our platform:</p><a href="${invitationLink}">${invitationLink}</a>`,
  }

  try {
    await sgMail.send(msg)
    console.log('Email sent')
  } catch (error) {
    console.error('Error sending email:', error)
  }
}

export async function sendInvitationEmailAndHandleErrors(): Promise<void> {
  try {
    const companyUrl = 'https://example.com' // need to change later
    const invitationLink = generateInvitationLink(companyUrl)

    const recipientEmail = 'recipient@example.com' // need to change later
    await sendInvitationEmail(recipientEmail, invitationLink)
  } catch (error) {
    console.error('An error occurred while sending invitation email:', error)
  }
}