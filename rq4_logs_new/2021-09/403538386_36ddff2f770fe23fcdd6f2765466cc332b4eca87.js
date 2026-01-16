import React, { useState } from 'react';
import '../css/Form.css';
 
 
// *** Form.js 에서 Hook(useState) 사용으로 인해 수정
// const Form = ({ value, onChange, onCreate, onKeyPress }) => {
const Form = ({ onCreate }) => {
 
    // React Hook > 클래스 타입에서는 사용 X
    const [ input, setInput ] = useState('');
 
    // input 값 변경
    const handleChange = (event) => {
        setInput(event.target.value);
    }
 
    // Enter key event
    const handleKeyPress = (event) => {
        // 눌려진 키가 Enter key 인 경우 handleCreate 호출
        if(event.key === 'Enter') {
            onCreate(input);
            setInput('');
        }
    }
 
 
    return (
        <div className="form">
            <input
                value={input}
                placeholder="오늘 할 일을 입력하세요.."
                onChange={handleChange}
                onKeyPress={handleKeyPress} />
            <div className="create-button" onClick={() => {
                    onCreate(input);
                    setInput('');
                }
            }>
                추가
            </div>
        </div>
    );
};
 
export default Form;