import { defineSchema, defineTable } from "convex/server"; 
import { v } from "convex/values"; 


export default defineSchema({
    message: defineTable({
        msg: v.string(),
        userId: v.string(),
        type: v.string(), // defines whether it's the chatbot or user's message
    })
    .index("byUserId",["userId"]),

    // user: defineTable({ 
    //     userId: v.string(),
    //     userName: v.string(),
    // })
    // .index("byUserId", ["userId"]),

    // chat: defineTable({
    //     userId: v.string(),
    //     timestamp: v.number(),
    // })
});