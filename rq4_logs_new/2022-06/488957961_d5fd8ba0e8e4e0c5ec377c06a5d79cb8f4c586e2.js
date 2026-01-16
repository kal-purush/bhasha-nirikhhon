import { useRef } from "react";
// Custom components
import useInput from "../../hooks/useInput";
import StepCard from "../Card/StepCard";
import InputField from "../Input/InputField";
// Assets
import "./RegistrationForm.scss";

const RegistrationForm = () => {

  const {
    inputValue: enteredEmail,
    inputIsValid: emailIsValid,
    inputError: emailHasError,
    setInputTouched: emailTouched,
    inputChangeHandler: emailChangeHandler,
    blurHandler: emailBlurHandler,
  } = useInput((inputValue) => inputValue.length >= 5 && inputValue.includes("@"));

  const {
    inputValue: enteredPassword,
    inputIsValid: passwordIsValid,
    inputError: passwordHasError,
    setInputTouched: passwordTouched,
    inputChangeHandler: passwordChangeHandler,
    blurHandler: passwordBlurHandler,
  } = useInput((inputValue) => inputValue.length > 5);

  const emailRef = useRef();
  const passwordRef = useRef();

  let emailErrMsg = "Email is required.";
  if (enteredEmail.length >= 1) {
    emailErrMsg =
      enteredEmail.length >= 5
        ? "Please enter a valid email address."
        : "Email should be between 5 and 50 characters.";
  }

  let passwordErrMsg =
    enteredPassword.length >= 1
      ? "Password should be between 6 and 60 characters long."
      : "Password is required.";

// Handler
  const submitHandler = (e) => {
    e.preventDefault();
    if (!emailIsValid) {
      emailTouched(true);
      passwordTouched(true);
      emailRef.current.focus();
      return;
    }

    if (!passwordIsValid) {
      passwordTouched(true);
      passwordRef.current.focus();
      return;
    }
    // redirect here & create new user
  };

  return (
    <form className="regForm">
      <StepCard
        className={"step step--large"}
        contentClass={"step__content step__content--large"}
        title={"STEP 1 OF 3"}
        subTitle={"Create a password to start your membership"}
        onClickHandler={submitHandler}
      >
        <p className="regForm__text">
          Just a few more steps and you're finished! We hate paperwork, too.
        </p>

        <InputField
          ref={emailRef}
          groupsClass={"inputField__groups inputField__groups--signup"}
          controlClass={"inputField__control inputField__control--error-sec"}
          type={"text"}
          label={"Email Address"}
          inputIsValid={emailIsValid}
          inputHasError={emailHasError}
          errorClass={"inputField__error inputField__error--error-sec"}
          errMsg={emailErrMsg}
          onBlurHandler={emailBlurHandler}
          onChangeHandler={emailChangeHandler}
        />

        <InputField
          ref={passwordRef}
          groupsClass={"inputField__groups inputField__groups--signup"}
          controlClass={"inputField__control inputField__control--error-sec"}
          type={"password"}
          label={"Password"}
          inputIsValid={passwordIsValid}
          inputHasError={passwordHasError}
          errorClass={"inputField__error inputField__error--error-sec"}
          errMsg={passwordErrMsg}
          onBlurHandler={passwordBlurHandler}
          onChangeHandler={passwordChangeHandler}
        />
      </StepCard>
    </form>
  );
};

export default RegistrationForm;