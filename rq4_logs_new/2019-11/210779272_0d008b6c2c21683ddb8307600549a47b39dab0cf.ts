import Email from 'email-templates';
import path from 'path';

import mailer from '../configs/instances/mailer';

export const email = new Email({
    message: {
        from: 'no-reply@stages.enib.fr',
    },
    // uncomment below to send emails in development/test env:
    // send: true
    transport: mailer,
    juiceResources: {
        preserveImportant: true,
        webResources: {
            relativeTo: path.resolve(__dirname, 'build'),
            images: true, // <--- set this as `true`
        },
    },

    // Change default template link "emails" for "templates"
    views: { path: path.join(__dirname, 'templates') },
});