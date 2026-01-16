from bson import ObjectId

from app.server.database import portfolio_collection, portfolio_helper


async def get_user_portfolio(user_id: str):
    portfolio = await portfolio_collection.find_one({"user_id": user_id})
    if portfolio:
        return portfolio_helper(portfolio)


async def add_portfolio(portfolio):
    portfolio = await portfolio_collection.insert_one(portfolio)
    new_portfolio = await portfolio_collection.find_one({"_id": portfolio.inserted_id})
    return portfolio_helper(new_portfolio)


async def update_portfolio(port_id, portfolio):
    old = await portfolio_collection.find_one({"_id": ObjectId(port_id)})
    if old:
        updated = await portfolio_collection.update_one(
            {"_id": ObjectId(port_id)}, {"$set": portfolio}
        )
        if updated:
            new_portfolio = await portfolio_collection.find_one({"_id": ObjectId(port_id)})
            return portfolio_helper(new_portfolio)
    return False


async def remove_portfolio(port_id):
    deleted = portfolio_collection.delete_one({"_id": ObjectId(port_id)})
    return deleted.deleted_count == 1