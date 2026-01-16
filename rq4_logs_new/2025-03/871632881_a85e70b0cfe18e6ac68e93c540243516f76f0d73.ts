const mongoose = require('mongoose');
import { Schema, model, models } from 'mongoose';

const jiraTicketSchema = new Schema({
    key: { type: String, required: true },
    summary: { type: String, required: true },
    status: {
        name: { type: String, required: true },
        issueStatus: { type: String, required: true }
    },
    priority: { type: String, required: true },
    assignee: { type: String },
    assigneeImage: { type: String },
    reported: { type: String },
    reportedImage: { type: String },
    link: { type: String, required: true },
    createdAt: { type: Date, required: true }
});
const jiraTicketsDetailSchema = new Schema({
    modifiedTicketList: {
        currentTickets: [String],
        newTickets: [String],
        qaTickets: [String],
        holdTickets: [String]
    },
    jiraTicketDetail: {
        currentTickets: [jiraTicketSchema],
        newTickets: [jiraTicketSchema],
        qaTickets: [jiraTicketSchema],
        holdTickets: [jiraTicketSchema]
    },
    fetchedAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
    deletedAt: { type: Date, default: null }
});

export const JiraTicketsDetail = models.JiraTicketsDetail || model('JiraTicketsDetail', jiraTicketsDetailSchema);




