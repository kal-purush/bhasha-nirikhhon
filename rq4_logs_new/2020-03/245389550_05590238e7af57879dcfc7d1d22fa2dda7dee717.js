import React from "react";
import { reviewBudget } from "../helpers";
import PropTypes from "prop-types";

const BudgetControl = ({ weeklyBudget, avaliableBudget }) => {
  return (
    <>
      <div className="alert alert-primary">Budget: ${weeklyBudget}</div>
      <div className={reviewBudget(weeklyBudget, avaliableBudget)}>
        Money Left: ${avaliableBudget}
      </div>
    </>
  );
};

BudgetControl.propTypes = {
  weeklyBudget: PropTypes.number.isRequired,
  avaliableBudget: PropTypes.number.isRequired
};

export default BudgetControl;