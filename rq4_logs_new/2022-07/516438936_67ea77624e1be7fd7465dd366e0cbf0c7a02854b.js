import React from 'react';

const SelectOptions = ({ options, defaultValue, value, onChange }) => {
    return (
        <select value={value}
        onChange={event=> onChange(event.target.value)}>
            <option disabled value=''>{defaultValue}</option>
            {options.map(option =>
                <option key={option.value} value={option.value}>
                    {option.name}
                </option>)}
        </select>
    )
}

// const SelectOptions = () => {
//     return (
//         <select>
//             <option disabled value=''>Количество элементов на странице</option>
//                 <option> 5 </option>
//                 <option> 10 </option>
//                 <option> Показать все </option>
//         </select>
//     )
// }


export default SelectOptions;