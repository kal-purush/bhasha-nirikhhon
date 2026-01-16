/* 
 * author : lelion21@gmail.com
 */

/**
 * Action Obj
 * @param {string} who
 * @param {string} type
 * @param {string} amount
 * @returns {void}
 */
function Action(who, type, amount) {

    this.type = type;   //  ["credit","debit"]  // A TESTER
    this.who = who;     //  ["driver", "owner", "insurance", "assistance", "drivy"]
    this.amount = amount;
    this.TYPES = {
        debit: "debit",
        credit: "credit"
    };
}
Action.prototype.get = function () {
    return {
        type: this.type,
        who: this.who,
        amount: this.amount
    };
};

//Action.prototype.setAmount = function () {
//    //this.amount = 0;
//
//
//};
//Action.prototype.getAmount = function () {
//    return this.amount;
//};
//Action.prototype.getDetail = function () {
//    return this.amount;
//};