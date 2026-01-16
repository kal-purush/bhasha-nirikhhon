import http from "k6/http";

export default function() {
    let data  = {url:"https:/exmaple.com"};
    let response = http.post("http://localhost:8080/short", JSON.stringify(data));
};