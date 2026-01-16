import { rest } from "msw";
import expenseTable from "./expenseTable.ts";
import * as dayjs from "dayjs";

const getExpenseHandler = rest.get("/api/expense", async (req, res, ctx) => {
  const lawsuitParam = Number.parseInt(
    req.url.searchParams.get("lawsuit") ?? "",
  );

  if (Number.isNaN(lawsuitParam)) {
    return res(ctx.status(400));
  }

  let expense = expenseTable.filter(
    (item) => item.lawsuitId === lawsuitParam && !item.isDeleted,
  );

  const startSpeningAtParam =
    req.url.searchParams.get("start-spening-at") ?? "";
  if (startSpeningAtParam !== "") {
    expense = expense.filter((item) =>
      dayjs(item.speningAt).isAfter(
        dayjs(startSpeningAtParam).subtract(1, "day"),
        "date",
      ),
    );
  }

  const endSpeningAtParam = req.url.searchParams.get("end-spening-at") ?? "";
  if (endSpeningAtParam !== "") {
    expense = expense.filter((item) =>
      dayjs(item.speningAt).isBefore(
        dayjs(endSpeningAtParam).add(1, "day"),
        "date",
      ),
    );
  }

  const contentsParam = req.url.searchParams.get("contents") ?? "";
  if (contentsParam !== "") {
    const regex = new RegExp(contentsParam, "i");
    expense = expense.filter((item) => regex.test(item.contents));
  }

  const startAmountParam = req.url.searchParams.get("start-amount") ?? "";
  if (startAmountParam !== "") {
    const startAmount = Number.parseInt(startAmountParam, 10);
    expense = expense.filter((item) => item.amount >= startAmount);
  }

  const endAmountParam = req.url.searchParams.get("end-amount") ?? "";
  if (endAmountParam !== "" && Number.parseInt(endAmountParam) !== 0) {
    const endAmount = Number.parseInt(endAmountParam, 10);
    expense = expense.filter((item) => item.amount <= endAmount);
  }
  if (endAmountParam !== "" && Number.parseInt(endAmountParam) === 0) {
    expense = expense.filter((item) => item.amount);
  }

  const pageParam = req.url.searchParams.get("page") ?? "0";
  const page = Number.parseInt(pageParam);

  const size = expense.length;

  expense = expense.sort((a, b) => b.id - a.id).slice(page * 5, (page + 1) * 5);

  return res(
    ctx.status(200),
    ctx.json({
      data: {
        expense,
        size,
      },
    }),
  );
});

const getDetailExpenseHandler = rest.get(
  "api/expense/:expenseId",
  async (req, res, ctx) => {
    const expenseId = Number.parseInt(req.params["expenseId"] as string) ?? "";

    console.log("api/expense/:expenseId = " + expenseId);

    if (Number.isNaN(expenseId)) {
      return res(ctx.status(400));
    }

    let data = expenseTable.filter(
      (item) => item.id === expenseId && !item.isDeleted,
    )[0];

    return res(ctx.status(200), ctx.json({ data }));
  },
);

// create
const postExpenseHandler = rest.post("/api/expense", async (req, res, ctx) => {
  const body: {
    lawsuitId: number;
    spendingAt: string;
    contents: string;
    amount: number;
  } = await req.json();

  expenseTable.push({
    id: expenseTable.length + 1,
    ...body,
    speningAt: new Date(body.spendingAt),
    isDeleted: false,
  });

  return res(ctx.status(200));
});

// update
const updateExpenseHandler = rest.put(
  "/api/expense/update/:expenseId",
  async (req, res, ctx) => {
    const body: {
      speningAt: string;
      contents: string;
      amount: number;
    } = await req.json();

    console.log(body.speningAt);
    const expenseId = Number.parseInt(
      (req.params["expenseId"] as string) ?? "",
    );

    if (Number.isNaN(expenseId)) {
      return res(ctx.status(400));
    }

    const foundExpense = expenseTable.filter(
      (item) => item.id === expenseId && !item.isDeleted,
    )[0];

    if (!foundExpense) {
      return res(ctx.status(400));
    }

    if (body["speningAt"] != undefined) {
      foundExpense.speningAt = new Date(body["speningAt"]);
    }

    if (body["contents"] != undefined) {
      foundExpense.contents = body["contents"];
    }

    if (body["amount"] != undefined) {
      foundExpense.amount = body["amount"];
    }

    return res(ctx.status(200), ctx.json({ data: foundExpense }));
  },
);

// delete
const deleteExpenseHandler = rest.put(
  "/api/expense/delete/:expenseId",
  async (req, res, ctx) => {
    const expenseId = Number.parseInt(
      (req.params["expenseId"] as string) ?? "",
    );

    if (Number.isNaN(expenseId)) {
      return res(ctx.status(400));
    }

    const foundExpense = expenseTable.filter(
      (item) => item.id === expenseId && !item.isDeleted,
    )[0];

    if (!foundExpense) {
      return res(ctx.status(400));
    }

    foundExpense.isDeleted = true;

    return res(ctx.status(200));
  },
);

const expenseHandlers = [
  getExpenseHandler,
  getDetailExpenseHandler,
  postExpenseHandler,
  updateExpenseHandler,
  deleteExpenseHandler,
];

export default expenseHandlers;