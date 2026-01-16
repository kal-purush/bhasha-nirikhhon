import { useRef, useState } from "react";
import { trackActions } from "../redux/store";
import { useDispatch, useSelector } from "react-redux";
const Balance = () => {
  const Product = useRef();
  const Amount = useRef();

  const [num, setNum] = useState(false);

  const modal = useSelector((state) => {
    return state.track.addData;
  });
  const profit = useSelector((state) => {
    return state.track.profit;
  });
  const expense = useSelector((state) => {
    return state.track.expense;
  });
  const total = useSelector((state) => {
    return state.track.totalValue;
  });
  const Radio = useSelector((state) => {
    return state.track.radioCheck;
  });
  const alert = useSelector((state) => {
    return state.track.alertform;
  });
  const chol = useSelector((state) => {
    return state.track.ch;
  });

  console.log(modal);
  const dispatch = useDispatch();
  const toggle = () => {
    dispatch(trackActions.toggleModal(true));
  };

  const toggle1 = () => {
    console.log("hey", Amount.current.value);
    if (
      Product.current.value.length > 0 &&
      !num &&
      Amount.current.value != 0 &&
      Radio
    ) {
      dispatch(trackActions.toggleModal(false));
      const obj = {
        id: Math.random(),
        amount: +Amount.current.value,
        product: Product.current.value,
      };
      dispatch(trackActions.addValueToData(obj));

      dispatch(trackActions.toggleModal(false));

      setTimeout(() => {
        dispatch(trackActions.setalter(0));
      }, 2000);
      dispatch(trackActions.setRadio(false));
    } else {
      dispatch(trackActions.setalter(2));
      setTimeout(() => {
        dispatch(trackActions.setalter(0));
      }, 1000);
      return;
    }
  };

  return (
    <div>
      <div className="border border-black">
        <div className="row w-75 mx-auto mt-4 py-2  d-flex  align-items-center ">
          {alert == 1 && (
            <h6 className="text-center pb-4 text-primary">
              ADD DATA TO CART....
            </h6>
          )}
          {alert == 2 && (
            <h6 className="text-center py-2 text-danger">
              PLZ FILL All THE FIELD .....
            </h6>
          )}
          {alert == 3 && (
            <h6 className="text-center py-2 text-danger">
              CREDIT ARE NOT ENOUGH FOR EXPENSES
            </h6>
          )}

          <div className=" col-12 col-md-8">
            <h6>TOTAL-BALANCE:$ {total}</h6>
          </div>
          <div className=" col-12 col-md-4 pb-1 text-center">
            <button type="button" class="btn btn-dark" onClick={toggle}>
              Add-Data
            </button>
          </div>
        </div>
        {modal && (
          <div className="w-75 mx-auto mt-4 p-4  border border-black">
            <div className="m-3">
              <input
                type="text"
                class="form-control"
                placeholder="Enter Product"
                maxlength="10"
                ref={Product}
              />
            </div>
            <div className="m-3">
              <input
                type="number"
                class="form-control"
                placeholder="Enter Amount"
                maxlength="5"
                onChange={(e) => {
                  if (
                    e.target.value <= 0 ||
                    e.target.value > 10000 ||
                    e.target.value == 0
                  ) {
                    setNum(true);
                  } else {
                    setNum(false);
                  }
                }}
                ref={Amount}
              />
              {num == true && (
                <p className="text-danger pt-2">Amount-Limit(1-10000)</p>
              )}
            </div>
            <div className="mx-3 d-flex row py-2">
              <div className=" col-12 col-md-4">
                <input
                  class="form-check-input"
                  type="radio"
                  name="exampleRadios"
                  id="exampleRadios1"
                  value="option1"
                  onChange={() => {
                    dispatch(trackActions.setRadioChecked("income"));
                    dispatch(trackActions.setRadio(true));
                  }}
                />
                <label className="form-check-label" for="exampleRadios1">
                  Income
                </label>
              </div>
              <div className=" col-12 col-md-6">
                <input
                  class="form-check-input"
                  type="radio"
                  name="exampleRadios"
                  id="exampleRadios1"
                  value="option1"
                  onChange={() => {
                    dispatch(trackActions.setRadioChecked("expense"));
                    dispatch(trackActions.setRadio(true));
                  }}
                />

                <label class="form-check-label" for="exampleRadios1">
                  Expense
                </label>
              </div>
            </div>
            <div className="pb-1 text-center">
              <button type="button" class="btn btn-dark m-1" onClick={toggle1}>
                Transaction
              </button>
            </div>
          </div>
        )}
      </div>
      <div className="row w-100 mx-auto  d-flex  align-items-center justify-content-center text-center">
        <div className="p-2 m-2 col-10 col-md-5 border border-black">
          <h4>INCOME</h4>
          <br></br>
          <h5>${profit}</h5>
        </div>
        <div className="p-2  m-2 col-10  col-md-5 border border-black">
          <h4>EXPENSE</h4>
          <br></br>
          <h5>${expense}</h5>
        </div>
      </div>
    </div>
  );
};
export default Balance;