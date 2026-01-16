
function setLocation(url)
{
    window.location = url;
}

function preventDoubleSubmit(elem)
{
    elem.submit.disabled=true;

    return true;
}