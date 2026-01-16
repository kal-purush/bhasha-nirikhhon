function activate(o)
{
o.style.borderColor='#57C5CE'
}
function disactivate(o,spec)
{
o.style.borderColor='#515150'
}
function wrimg(s,n,nm)
{
document.write("<img id='"+l[n-1]+"' class='smal pont' src='"+s+n+"/"+nm+".jpg'",
" border='2px' onmouseover='activate(this)' onmouseout='disactivate(this)'",
"onclick='parent.vision(this);parent.lim=this.id'>")
}