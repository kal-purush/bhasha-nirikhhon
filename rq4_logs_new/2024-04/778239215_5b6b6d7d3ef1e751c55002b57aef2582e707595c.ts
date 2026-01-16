import http from 'http';
import express from 'express';
import { Server as SocketIOServer } from 'socket.io';
import path from 'path';

const app = express();
const server = http.createServer(app as http.RequestListener);
const io = new SocketIOServer(server);

app.use(express.static(path.join(__dirname, 'static')));

const sessions = new Map<string, SessionState>();

class TimerState {
    endTime: Date | null = null;
    timeoutRef: ReturnType<typeof setTimeout> | null = null;
    readonly authorizedSIDs = new Set<string>();
    readonly punishedSIDs = new Set<string>();
    pin: string | null = null;
    minutes = 0;
    seconds = 0;

    clear() {
        this.endTime = null;
        this.timeoutRef = null;
        this.authorizedSIDs.clear();
        this.punishedSIDs.clear();
        this.pin = null;
        this.minutes = 0;
        this.seconds = 0;
    }

    copyFrom(other: TimerState) {
        this.endTime = other.endTime;
        this.timeoutRef = other.timeoutRef;
        this.authorizedSIDs.clear();
        this.punishedSIDs.clear();
        other.authorizedSIDs.forEach(sid => this.authorizedSIDs.add(sid));
        this.pin = other.pin;
        this.minutes = other.minutes;
        this.seconds = other.seconds;
    }

    start(io: SocketIOServer, sessionID: string) {
        if (!this.timeoutRef) {
            const now = Date.now();
            this.endTime = new Date(now + this.minutes * 60000 + this.seconds * 1000);
            const duration = this.endTime.getTime() - now;
            this.timeoutRef = setTimeout(() => this.tick(io, sessionID), duration);
        }
    }

    punish(sid: string) {
        this.punishedSIDs.add(sid);
        setTimeout(() => this.punishedSIDs.delete(sid), 3000);
    }

    getRemainingTime() {
        if (!this.endTime) {
            return { minutes: this.minutes, seconds: this.seconds };
        }
        let remainingTime = this.endTime.getTime() - Date.now();
        if(remainingTime < 0) {
            remainingTime = 0;
        }
        return {
            minutes: Math.floor(remainingTime / 60000),
            seconds: Math.floor((remainingTime % 60000) / 1000)
        };
    }

    isLocked() {
        return !!(this.pin && this.pin.length >= 6);
    }

    isLockedFor(sid: string) {
        //If we aren't locked for anyone, we aren't locked for this specific sid either.
        if(!this.isLocked()) {
            return false;
        }
        else {
            //We're locked; determine if we're locked for this specific SID
            if(this.authorizedSIDs.has(sid)) {
                //This user is authorized, so they're not locked
                return false;
            }
            else {
                //This user is not authorized, so they're locked
                return true;
            }
        }
    }

    stop() {
        if (this.timeoutRef) {
            clearTimeout(this.timeoutRef);
            this.timeoutRef = null;
        }
        if(this.endTime) {
            let grt = this.getRemainingTime();
            this.minutes = grt.minutes;
            this.seconds = grt.seconds;
        }
        this.endTime = null;
    }

    tick(io: SocketIOServer, sessionID: string) {
        if (!this.endTime) return;
        let justFinished = false;
        if (Date.now() >= this.endTime.getTime()) {
            this.minutes = 0;
            this.seconds = 0;
            justFinished = true;
            this.stop();
        } else {
            const remaining = this.endTime.getTime() - Date.now();
            this.minutes = Math.floor(remaining / 60000);
            this.seconds = Math.floor((remaining % 60000) / 1000);
            const nextTick = Math.min(remaining, 1000);
            this.timeoutRef = setTimeout(() => this.tick(io, sessionID), nextTick);
        }
        emitTimerUpdate(io, sessionID, sessionID, justFinished);
    }
}

class SessionState {
    readonly timerState = new TimerState();
    readonly previousTimerState = new TimerState();
    clients = new Set<string>();
}

function emitTimerUpdate(socket: SocketIOServer, sessionID: string, target: string, justFinished: boolean = false) {
    console.log(`INFO: Emitting timer update to ${target}`);
    const session = sessions.get(sessionID);
    if (!session) return;
    let grt = session.timerState.getRemainingTime();
    const emitTo = (sid: string) => {
        console.log(`INFO: emitTo called with sid ${sid}`);
        const dataToPush = {
            sessionId: sessionID,
            timerState: {
                minutes: grt.minutes,
                seconds: grt.seconds,
                running: session.timerState.timeoutRef !== null,
                justFinished: justFinished
            },
            locked: session.timerState.isLocked(),
            lockedForMe: session.timerState.isLockedFor(sid)
        };
        console.log(`INFO: Emitting to ${sid} with data ${JSON.stringify(dataToPush)}`);
        io.to(sid).emit('timer_update', dataToPush);
    };

    if (target === sessionID) {
        session.clients.forEach(emitTo);
    } else {
        emitTo(target);
    }
}

function checkPunish(sessionID: string, sid: string) {
    let isPunished = sessions.get(sessionID)?.timerState.punishedSIDs.has(sid);
    if (isPunished) {
        console.log(`WARN: Client ${sid} attempted to perform action on session ${sessionID} while punished`);
        return true;
    }
    return false;
}

function hasPermission(sessionID: string, sid: string) {
    const session = sessions.get(sessionID);
    if (!session || session.clients.size === 0) {
        console.log(`WARN: Client ${sid} attempted to perform action on invalid session ${sessionID}`);
        return false;
    }
    if (session.timerState.isLocked()) {
        if (session.timerState.isLockedFor(sid)) {
            console.log(`WARN: Client ${sid} attempted to perform action on locked session ${sessionID} without permission`);
            return false;
        }
    }
    console.log(`INFO: Client ${sid} has permission to perform action on session ${sessionID}`);
    return true;
}

io.on('connection', async (socket) => {
    const sessionID = socket.handshake.auth.sessionId as string;
    console.log(`INFO: INITIAL Client ${socket.id} connected with session ${sessionID}`);

    if (sessionID && sessionID.length < 512) {
        let session = sessions.get(sessionID);
        if (!session) {
            session = new SessionState();
            sessions.set(sessionID, session);
        }
        session.clients.add(socket.id);
        socket.join(sessionID);
        socket.join(socket.id);

        console.log(`INFO: END Client ${socket.id} connected with session ${sessionID}`);
    } else {
        console.log(`WARN: Client ${socket.id} connected without session.`);
        return;
    }

    const handleToggleLockEvent = async (data: { pin: string; unlockFor: 'me' | 'all' }) => {
        console.log(`INFO: Client ${socket.id} attempted to toggle lock`);
        if (!data) {
            console.log("WARN: Client attempted to toggle lock without data")
            return;
        }
        const { pin, unlockFor } = data;

        const session = sessions.get(sessionID);
        if (!session) return;

        if(checkPunish(sessionID, socket.id)) {
            return;
        }

        if (session.timerState.isLocked()) {
            let bhv = await Bun.password.verify(pin, session.timerState.pin!);
            if (bhv) {
                switch (unlockFor) {
                    case 'me':
                        session.timerState.authorizedSIDs.add(socket.id);
                        emitTimerUpdate(io, sessionID, socket.id);
                        console.log("case me");
                        break;
                    case 'all':
                        session.timerState.pin = null;
                        session.timerState.authorizedSIDs.clear();
                        emitTimerUpdate(io, sessionID, sessionID);
                        console.log("case all")
                        break;
                    default:
                        console.log(`WARN: Client ${socket.id} attempted to unlock session ${sessionID} with invalid unlockFor value ${unlockFor}!`);
                        break;
                }
            } else {
                console.log(`WARN: Client ${socket.id} attempted to unlock session ${sessionID} with incorrect PIN!`);
                session.timerState.punish(socket.id);
            }
        } else if (pin && pin.length >= 6 && /\d{6,8}/.test(pin)) {
            let hash = await Bun.password.hash(pin);
            session.timerState.pin = hash;
            session.timerState.authorizedSIDs.clear();
            session.timerState.authorizedSIDs.add(socket.id);
            emitTimerUpdate(io, sessionID, sessionID);
            console.log("case made the session locked")
        } else {
            console.log(`WARN: Client ${socket.id} attempted to lock session ${sessionID} with invalid PIN!`);
        }
    };

    const handleDisconnect = () => {
        console.log(`INFO: Client ${socket.id} disconnected`);
        if (!sessionID) {
            return;
        }

        const session = sessions.get(sessionID);
        if (!session) return;

        session.clients.delete(socket.id);
        if (session.clients.size === 0) {
            sessions.delete(sessionID);
        }
    };

    const handleStartTimerEvent = () => {
        console.log(`INFO: Client ${socket.id} attempted to start timer`);
        if (!hasPermission(sessionID, socket.id) || checkPunish(sessionID, socket.id)) {
            return;
        }
        const session = sessions.get(sessionID);
        if (!session) return;
        session.timerState.start(io, sessionID);
        session.previousTimerState.copyFrom(session.timerState);
        emitTimerUpdate(io, sessionID, sessionID);
    };

    const handleStopTimerEvent = () => {
        console.log(`INFO: Client ${socket.id} attempted to stop timer`);
        if (!hasPermission(sessionID, socket.id) || checkPunish(sessionID, socket.id)) {
            return;
        }
        const session = sessions.get(sessionID);
        if (!session) return;
        session.timerState.stop();
        emitTimerUpdate(io, sessionID, sessionID);
    };

    const handleResetTimerEvent = () => {
        console.log(`INFO: Client ${socket.id} attempted to reset timer`);
        if (!hasPermission(sessionID, socket.id) || checkPunish(sessionID, socket.id)) {
            return;
        }
        const session = sessions.get(sessionID);
        if (!session) return;
        session.timerState.endTime = null;
        session.timerState.timeoutRef = null;
        session.timerState.minutes = session.previousTimerState.minutes;
        session.timerState.seconds = session.previousTimerState.seconds;
        emitTimerUpdate(io, sessionID, sessionID);
    };

    const handleClearTimerEvent = () => {
        console.log(`INFO: Client ${socket.id} attempted to clear timer`);
        if (!hasPermission(sessionID, socket.id) || checkPunish(sessionID, socket.id)) {
            return;
        }
        const session = sessions.get(sessionID);
        if (!session) return;
        session.timerState.clear();
        emitTimerUpdate(io, sessionID, sessionID);
    };

    const handleSetTimerEvent = (data: { unit: 'minutes' | 'seconds'; value: number }) => {
        console.log(`INFO: Client ${socket.id} attempted to set timer`);
        if (!hasPermission(sessionID, socket.id) || checkPunish(sessionID, socket.id)) {
            return;
        }
        if (!data) {
            return;
        }
        const { unit, value } = data;

        const session = sessions.get(sessionID);
        if (!session) return;
        session.previousTimerState.copyFrom(session.timerState);
        switch (unit) {
            case 'minutes':
                session.timerState.minutes = value;
                break;
            case 'seconds':
                session.timerState.seconds = value;
                break;
        }
        emitTimerUpdate(io, sessionID, sessionID);
    };

    const handleIncrementTimerEvent = (data: { unit: 'minutes' | 'seconds'; direction: 'up' | 'down' }) => {
        console.log(`INFO: Client ${socket.id} attempted to increment timer`);
        if (!hasPermission(sessionID, socket.id) || checkPunish(sessionID, socket.id)) {
            return;
        }
        if (!data) {
            console.log(`WARN: Client ${socket.id} attempted to increment timer without data!`);
            return;
        }
        const { unit, direction } = data;
        const validUnits = ['minutes', 'seconds'];
        const validDirections = ['up', 'down'];

        const isValidUnit = validUnits.includes(unit);
        const isValidDirection = validDirections.includes(direction);
        if (!isValidUnit || !isValidDirection) {
            console.log(`WARN: Client ${socket.id} attempted to increment timer with invalid unit or direction!`);
            return;
        }

        const session = sessions.get(sessionID);
        if (!session) {
            console.log(`WARN: handleIncrementTimerEvent: Don't know about session ${sessionID}`);
            return;
        }

        console.log(`INFO: Incrementing timer ${unit} ${direction}`);

        switch (unit) {
            case 'seconds':
                if (direction === 'up') {
                    console.log("DEBUG: 1");
                    session.timerState.seconds++;
                } else {
                    console.log("DEBUG: 2");
                    session.timerState.seconds--;
                }
                if (session.timerState.seconds < 0) {
                    console.log("DEBUG: 3");
                    session.timerState.seconds = 59;
                    session.timerState.minutes--;
                } else if (session.timerState.seconds >= 60) {
                    console.log("DEBUG: 4");
                    session.timerState.seconds = 0;
                    session.timerState.minutes++;
                }
                break;
            case 'minutes':
                if (direction === 'up') {
                    console.log("DEBUG: 5");
                    session.timerState.minutes++;
                } else {
                    console.log("DEBUG: 6");
                    session.timerState.minutes--;
                }
                break;
            default:
                console.log("WARN: Invalid unit");
        }

        console.log(`INFO: ${session.timerState.minutes} minutes, ${session.timerState.seconds} seconds`);
        emitTimerUpdate(io, sessionID, sessionID);
    };

    const handleGetTimerEvent = () => {
        console.log(`INFO: Client ${socket.id} attempted to get timer`);
        if(checkPunish(sessionID, socket.id)) {
            return;
        }
        if (sessions.has(sessionID)) {
            emitTimerUpdate(io, sessionID, socket.id);
        } else {
            console.log(`WARN: Client ${socket.id} attempted to get timer for session ${sessionID} that does not exist`);
        }
    };

    socket.on('disconnect', handleDisconnect);
    socket.on('start_timer', handleStartTimerEvent);
    socket.on('stop_timer', handleStopTimerEvent);
    socket.on('reset_timer', handleResetTimerEvent);
    socket.on('clear_timer', handleClearTimerEvent);
    socket.on('set_timer', handleSetTimerEvent);
    socket.on('increment_timer', handleIncrementTimerEvent);
    socket.on('toggle_lock', handleToggleLockEvent);
    socket.on('get_timer', handleGetTimerEvent);
});

const port = 9001;
server.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});