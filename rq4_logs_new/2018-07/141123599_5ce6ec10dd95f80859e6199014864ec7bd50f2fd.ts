const getExpense = async (_, { input }, ctx) => {
  console.log(ctx.models, 'ctx');
  const expense = await ctx.models.expense.findOne(input.id);
  if (!expense) {
    throw new Error('Expense does not exist');
  }
  return expense;
};

const createExpense = async (_, { input }, ctx) => {
  const expense = new ctx.models.expense();
  expense.name = input.name;
  expense.category = input.category;
  expense.amount = input.amount;
  expense.expenseInfo = input.expenseInfo;
  expense.user = input.userId;

  try {
    const newExpense = await expense.save();
    return newExpense;
  } catch (error) {
    return error;
  }
};

module.exports = {
  Query: {
    getExpense,
  },
  Mutation: {
    createExpense,
  },
  Expense: {
    user: async (expense, _ , ctx) => {
      const expenseOwner = ctx.models.user.findOne(expense.userId);
      return expenseOwner;
    },
  },
};