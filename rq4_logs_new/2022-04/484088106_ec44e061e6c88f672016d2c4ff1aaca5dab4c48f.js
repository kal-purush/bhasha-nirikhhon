// 새 터미널 => npm i prop-types (PropTypes 설치)
import PropTypes from "prop-types";
import styles from "./Button.module.css";

function Button({text}){
    return <button className={styles.title}>{text}</button>;
}
Button.propTypes = {
    text: PropTypes.string.isRequired,
}

export default Button;