function datetimestringtodate(datetime)
{
    var m = re.exec(kelime);
    var d = new Date(parseInt(m[0]));
    return d;

}
function datetostring(d)
{
  return d.getDate() + "."+d.getMonth()+"."+d.getFullYear() + " " + d.getHours() + ":" + d.getMinutes();
}