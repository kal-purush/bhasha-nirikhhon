import { Injectable } from '@nestjs/common';
import { SendGridService } from "@anchan828/nest-sendgrid";
import { mailMessage } from './messages/mail.messages';

@Injectable()
export class MailService {
    constructor(private readonly sendGrid: SendGridService) {}

    async emailVerification(email, firstName, url): Promise<void> {
        await this.sendGrid.send({
            to: `${email}`,
            from: 'Manchee <okwy23@gmail.com>',
            subject: "Email Verification",
            text: mailMessage.verificationMessageText(firstName, url),
            html: mailMessage.verificationMessageHtml(firstName, url),
        });
    }
}