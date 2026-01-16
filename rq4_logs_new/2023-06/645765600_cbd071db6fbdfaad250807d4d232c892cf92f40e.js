// viet hooks luu thong tin cua nguoi dung 
// vao localstorage
import { useState } from "react";

export const useLocalStorage = (keyName, defaultValue) => {
    // muon luu tru du lieu vao local storage tren trinh duyet.
    // khai bao key va du lieu tren trinh duyet la gi ?
    const [storeValue, setStoreValue] = useState(() => {
        try {
            // xu li 
            // 1 - lay gia tri tu local storage trong trinh duyet (neu co)
            const value = window.localStorage.getItem(keyName);
            if (value) {
                //co gia tri
                // tra du lieu ve cho state storeValue
                // khi luu du lieu vao local storage thi trinh duyet chi chap nhan dinh dang la string (chuoi)
                // chuyen tu chuoi json ve object
                return JSON.parse(value);
            } else {
                //ko co gia tri
                // luu data vao local storage
                window.localStorage.setItem(keyName, JSON.stringify(defaultValue));
                return defaultValue;
            }
        } catch (error) {
            // bat loi
            console.log(error);
            return defaultValue; // tra gia tri ve cho state storeValue
        }
    })
    // xu li cho phan setStoreValue (function cap nhat lai state storeValue)
    const setValue = (newValue) => {
        try {
            // xu li
            window.localStorage.setItem(keyName, JSON.stringify(newValue));
        } catch (error) {
            // bao loi
            console.log(error);
        }
        setStoreValue(newValue);
    }
    return [storeValue, setStoreValue];
}