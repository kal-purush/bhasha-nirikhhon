from flask import Flask, jsonify, request

from .model.expense import Expense, ExpenseSchema
from .model.income import Income, IncomeSchema
from .model.transaction_type import TransactionType

import json
import requests

app = Flask(__name__)

transactions = [
    Income('Salary', 5000),
    Income('Dividends', 200),
    Expense('pizza', 50),
    Expense('Rock Concert', 100)
]


@app.route('/incomes')
def get_incomes():
    schema = IncomeSchema(many=True)
    incomes = schema.dump(
        filter(lambda t: t.type == TransactionType.INCOME, transactions)
    )
    return jsonify(incomes)


@app.route('/api/ping')
def add_income():
    return "{success: true}", 200


@app.route('/api/posts')
def add_expenses():
    tags = request.args.get('tags')
    print(request.args)
    sortBy = request.args.get('sortBy')
    direction = request.args.get('direction')

    response = requests.get("https://api.hatchways.io/assessment/blog/posts", params={'tag': tags})

    json_data = json.loads(response.text)

    for x, y in json_data.items():

        print(x)

    return str(response.text)


"""
@app.route('/expenses')
def get_expenses():
  schema = ExpenseSchema(many=True)
  expenses = schema.dump(
      filter(lambda t: t.type == TransactionType.EXPENSE, transactions)
  )
  print("hello test", expenses)

  return jsonify(expenses.data)
"""


@app.route('/expenses', methods=['POST'])
def add_expense():
    expense = ExpenseSchema().load(request.get_json())
    transactions.append(expense.data)
    return "", 204


if __name__ == "__main__":
    app.run()