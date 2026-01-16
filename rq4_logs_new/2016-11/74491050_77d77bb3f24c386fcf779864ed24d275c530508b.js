import express from 'express';
import cors from 'cors';
import hslToHex from 'hsl-to-hex'

function rgbToHex(r, g, b) {
  if ( r > 255 || g > 255 || b > 255 )
    return 'Invalid color'
  console.log(r, g,b)
  if ( isNaN(r) || isNaN(g) || isNaN(b) )
    return 'Invalid color'
  return  "#" + ((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1);

}

function rgb( str ) {
  var color = str.split(', ');
  if ( color.length !== 3 )
    return 'Invalid color'
  return rgbToHex( Number(color[0]), Number(color[1]), Number(color[2]));
}

function hls( str ) {
  var color = str.split(',%20');
  if ( color.length !== 3 )
    return 'Invalid color';
  if ( color[1].charAt(color[1].length - 1) != '%' || color[2].charAt(color[2].length - 1) != '%')
    return 'Invalid color';
  if ( Number(color[0]) > 359 || Number(color[1].slice(0, color[1].length - 1 )) > 100 ||
    Number(color[2].slice(0, color[2].length - 1 )) > 100 ||
    Number(color[0]) < 0 || Number(color[1].slice(0, color[1].length - 1 )) < 0 ||
    Number(color[2].slice(0, color[2].length - 1 )) < 0 )
      return 'Invalid color';
  return hslToHex( Number(color[0]), Number(color[1].slice(0, color[1].length - 1)),
    Number(color[2].slice(0, color[2].length - 1)) )
}

function checkColor(color) {

  color = color.trim();
  if ( color.slice(0, 3) == 'rgb' ) {
    return (rgb(color.slice(4, color.length - 1)));
  }
  console.log(color);
  if ( color.slice(0, 3) == 'hsl' ) {
    return (hls(color.slice(4, color.length - 1)));
  }

  if ( color[0] == '#' )
    color = color.slice(1, color.length)
  var matchColor = color.match(/(([0-9a-fA-F]{3})?[0-9a-fA-F]{3})/);
  if ( !matchColor )
    return 'Invalid color';

  matchColor = matchColor.slice(0, 1).toString();
  if ( matchColor.length !== 6 && matchColor.length !== 3 )
    return 'Invalid color';

  if ( color.length !== matchColor.length )
    return 'Invalid color';
  var matchColor = matchColor.replace( /A/g, "a" ).replace( /B/g, "b" ).replace( /C/g, "c" ).
     replace( /D/g, "d" ).replace( /E/g, "e" ).replace( /F/g, "f" );
  if ( matchColor.length == 3 )
    return ( '#' + matchColor[0] + matchColor[0] + matchColor[1] +
      matchColor[1] + matchColor[2] + matchColor[2] );
  else
    return '#' + matchColor
}

const app = express();
app.use(cors());
app.get('/', (req, res) => {
  res.json({
    hello: 'JS World',
  });
});

app.get('/task2d', (req, res) => {
  if ( req.url.search(/\?color=/) === -1 )
    res.send('Invalid color');
  const resultColor = checkColor(req.query.color);
  res.send(resultColor);
})

app.listen(3000, () => {
  console.log('Your app listening on port 3000!');
});