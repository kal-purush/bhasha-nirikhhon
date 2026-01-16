import { auditLog } from '@audit/handlers/audit-stream';


jest.mock('@audit/utils/elasti-client');

import { ESClient } from '@audit/utils/elasti-client';
import { AssessAuditEvent } from '@audit/models/event';

describe("Audit Kinesis Handler", () => {
    const records1 = [
        {
            username: "mpaul",
            userId: "A1000",
            eventDate: 123456,
            eventType: "sometype1",
            area: "area1",
            clientId: "CL0001",
            clientName: "Jane",
            deviceId: "asads100",
            deviceName: "ipAd",
            entity: "Internal QA",
            bu: "US BU",
            offline: true,
            changes: [
                {
                    fieldName: "fr",
                    oldValue: "old",
                    newValue: "new"
                }
            ]
        },
        {
            username: "upaulm2",
            userId: "B1000",
            eventDate: 45555,
            eventType: "sometype2",
            area: "area2",
            clientId: "CL0002",
            clientName: "Joe",
            deviceId: "asads200",
            deviceName: "Android",
            offline: false,
            entity: "Internal QA1",
            bu: "CA BU"
        }
    ];

    const records2 = [
        {
            username: "x1000",
            userId: "X1000",
            eventDate: 9099,
            eventType: "sometype3",
            area: "area3",
            clientId: "CL0003",
            clientName: "T1000",
            deviceId: "asads700",
            deviceName: "BBerry",
            offline: false,
            entity: "WeylandCorp",
            bu: "Mars BU"
        }

    ];

    it("should call elastisearch client with record data", () => {
        let _logs: AssessAuditEvent[] = [];
        ESClient.prototype.processAssessLogs = jest.fn().mockImplementation((logs: AssessAuditEvent[]) => {
            logs.forEach(l => _logs.push(l));
        });
        let passedContent: any = {};
        let context: any = {};
        auditLog({
            Records: [
                {
                    kinesis: {
                        data: Buffer.from(JSON.stringify(records1, null, 5)).toString('base64')
                    }
                },
                {
                    kinesis: {
                        data: Buffer.from(JSON.stringify(records2, null, 5)).toString('base64')
                    }
                }
            ]
        }, context, (arg1: any, content: any) => {
            passedContent = content;
        });
        const original = records1.map(it => it);
        original.push(records2[0]);
        expect(_logs).toEqual(original);
        expect(passedContent).toBe('Successfully processed 2 event.');
    });


});