import { test, expect } from "@playwright/test";
import { createSMSPromiseList } from "../../../../build/_common/lib/createBulkSMSSend.mjs";

const sampleTenantList = [
  { phoneNumber: "+11234567890", bodyText: "Hello, World!" },
  { phoneNumber: "+11234567890", bodyText: "Hello, World!" },
  { phoneNumber: "+11234567890", bodyText: "Hello, World!" },
];

// TODO: Investigate tests for Promises
// Promise executor functions are run automatically upon creation