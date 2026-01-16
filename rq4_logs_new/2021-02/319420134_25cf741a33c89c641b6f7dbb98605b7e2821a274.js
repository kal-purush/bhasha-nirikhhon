const helpers = {};

helpers.formatDate = (date) => {
    console.log(date);
    let a = date.split("T");
    let d = a[0].split("-");
    let t = a[1].split(":");        

    let ampm = (t[0] > 12) ? 'PM' : 'AM';
    let hour = t[0] - 12;

    return `${d[1]}/${d[2]}/${d[0]} at ${hour}:${t[1]} ${ampm}`;    
}

export default helpers;