import * as v from "valibot";
import { nl } from "../nl";
import { Tag } from ".";

type Output = v.InferOutput<typeof nl.expense.config.schema>;

type RoleOptions = "payer" | "participant";

type OutputWithRoleConstraints = Output & {
  members: { role: RoleOptions }[];
};
const USER = {
  name: "USER",
  role: "payer",
  split: 0.5,
} as const;

function entry(
  input: string,
  expense: OutputWithRoleConstraints["expense"],
  members: OutputWithRoleConstraints["members"],
  tags?: Tag[]
) {
  const t: Tag[] = tags ? [...tags, "description.core"] : ["description.core"]; // TODO: check core isn't dup
  return { input, expected: { expense, members }, tags: t };
}

export const data = [
  entry(
    "Between John and I, that dinner came to $85",
    { amount: 85, description: "Dinner" },
    [
      USER,
      {
        name: "John",
        role: "participant",
        split: 0.5,
      },
    ]
  ),
  entry(
    "Got Annie a haircut, split with Jane Doe, $71",
    { amount: 71, description: "Annie's haircut" },
    [
      USER,
      {
        name: "Jane Doe",
        role: "participant",
        split: 0.5,
      },
    ],
    ["description.include-names-when-not-members"]
  ),
  entry(
    "Split beans and coffee with Jane Doe, $29",
    { amount: 29, description: "Beans and coffee" },
    [
      USER,
      {
        name: "Jane Doe",
        role: "participant",
        split: 0.5,
      },
    ]
  ),
  entry(
    "Grocery store this week cost 67. Split with John.",
    { amount: 67, description: "Grocery store" },
    [
      USER,
      {
        name: "John",
        role: "participant",
        split: 0.5,
      },
    ],
    ["description.omit-date-time-info"]
  ),
  entry(
    "Movie tickets on Saturday with Jane, $30",
    { amount: 30, description: "Movie tickets" },
    [
      USER,
      {
        name: "Jane",
        role: "participant",
        split: 0.5,
      },
    ],
    ["description.omit-date-time-info"]
  ),
  entry(
    "Cleaning supplies at Superstore with John. $23",
    { amount: 23, description: "Cleaning supplies at Superstore" },
    [
      USER,
      {
        name: "John",
        role: "participant",
        split: 0.5,
      },
    ]
  ),
];