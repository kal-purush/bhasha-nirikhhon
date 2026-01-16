// IMPORTS
// ================================================================================================
import { Server } from 'socket.io';
import { Notifier, Notice, Logger, validate, util } from 'nova-base';

// NOTIFIER CLASS
// ================================================================================================
export class SocketNotifier implements Notifier {

    server  : Server;
    logger? : Logger;

    // CONSTRUCTOR
    // --------------------------------------------------------------------------------------------
    constructor(server: Server, logger?: Logger) {
        if (!server) throw new Error('Cannot create socket notifier: server is undefined');
        this.server = server;
        this.logger = logger;
    }

    // PUBLIC METHODS
    // --------------------------------------------------------------------------------------------
    send(noticeOrNotices: Notice | Notice[]) {
        validate(noticeOrNotices, 'Cannot send notices: notices are undefined');
        const notices = Array.isArray(noticeOrNotices) ? noticeOrNotices : [noticeOrNotices];
        if (notices.length === 0) return Promise.resolve();

        const start = process.hrtime();
        this.logger && this.logger.debug(`Sending (${notices.length}) notices...`);
        for (let notice of notices) {
            if (!notice) continue;
            
            if (notice.topic) {
                this.logger && this.logger.debug(`Sending ${notice.topic}:${notice.event} notice to (${notice.target}) target`);
                this.server.of(notice.topic).in(notice.target).emit(notice.event, notice.payload);
            }
            else {
                this.logger && this.logger.debug(`Sending ${notice.event} notice to (${notice.target}) target`);
                this.server.sockets.in(notice.target).emit(notice.event, notice.payload);
            }
        }

        // log the notice sent event
        this.logger && this.logger.log(`Notices Sent`, {
            count   : notices.length,
            time    : util.since(start)
        });
        
        // return success
        return Promise.resolve();
    }
}