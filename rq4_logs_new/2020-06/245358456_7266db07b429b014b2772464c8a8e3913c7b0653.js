/** @jsx jsx */
import { jsx, css } from "@emotion/core";
import { useState } from "react";
import addToMailchimp from "gatsby-plugin-mailchimp";
import { colors, smBreak } from "../_Theme/UpdatedBrandTheme";

const formItem = css``;

const visuallyHidden = css`
  position: absolute !important;
  top: -9999px !important;
  left: -9999px !important;
`;

const resultMsg = css`
  position: absolute;
  output {
    font-size: 1rem;
  }
`;

function Mailchimp() {
  const [resultMessage, setResultMessage] = useState("");
  const [error, setError] = useState(false);
  const [FNAME, setFNAME] = useState("");
  const [LNAME, setLNAME] = useState("");
  const [EMAIL, setEMAIL] = useState("");

  const resultMessageClass = error
    ? css`
        color: ${colors.pink.hex};
      `
    : css`
        color: ${colors.primary.hex};
      `;

  const handleSubmit = e => {
    e.preventDefault();
    const email = EMAIL;
    const fields = {
      FNAME,
      LNAME
    };
    addToMailchimp(email, fields).then(result => {
      setResultMessage(result.msg.split("<a")[0]);
      setError(result.result === "error");
    });
  };

  return (
    <form onSubmit={handleSubmit} name="emailForm">
      <div
        css={css`
          margin: 1rem auto 2rem;
          display: grid;
          grid-template-columns: 2fr 2fr 3.5fr;
          grid-column-gap: 40px;
          width: 100%;

          div > label > input {
            height: 50px;
            border: 2px solid ${colors.medium.hex};
            width: 95%;

            &:focus {
              outline: none;
              border: 3px solid ${colors.blue.hex};
              margin-top: -2px;
            }

            ::placeholder {
              color: ${colors.plumLight};
            }
          }

          ${smBreak} {
            margin: 45px auto 25px;
            grid-template-columns: 1fr;
            grid-template-rows: repeat(3, 1fr);
            grid-row-gap: 20px;
            width: 100%;
          }
        `}
      >
        <div css={formItem}>
          <label htmlFor="fname">
            <span css={visuallyHidden}>First Name</span>
            <input
              autoComplete="given-name"
              id="fname"
              name="FNAME"
              type="text"
              value={FNAME}
              placeholder="First name"
              onChange={event => setFNAME(event.target.value)}
              className={FNAME.length > 0 ? "labelAbove" : ""}
            />
          </label>
        </div>
        <div css={formItem}>
          <label htmlFor="lname">
            <span css={visuallyHidden}>Last Name</span>
            <input
              autoComplete="family-name"
              id="lname"
              name="LNAME"
              type="text"
              value={LNAME}
              placeholder="Last name"
              onChange={event => setLNAME(event.target.value)}
              className={LNAME.length > 0 ? "labelAbove" : ""}
            />
          </label>
        </div>
        <div css={formItem}>
          <label htmlFor="email">
            <span css={visuallyHidden}>Email Address</span>
            <input
              aria-describedby="email-error"
              aria-required="true"
              autoComplete="email"
              aria-invalid={error}
              id="email"
              name="EMAIL"
              type="email"
              value={EMAIL}
              placeholder="Email (required)"
              onChange={event => setEMAIL(event.target.value)}
              className={EMAIL.length > 0 ? "labelAbove" : ""}
            />
          </label>
          <div css={resultMsg}>
            <output id="email-error" css={resultMessageClass}>
              {resultMessage}
            </output>
          </div>
        </div>
      </div>
      <div
        css={css`
          width: 100%;
          display: grid;
        `}
      >
        <button
          type="submit"
          className="btn-purple"
          formNoValidate
          css={css`
            justify-self: end;
            ${smBreak} {
              justify-self: center;
            }
          `}
        >
          <p>Sign Up</p>
        </button>
      </div>
    </form>
  );
}

export default Mailchimp;