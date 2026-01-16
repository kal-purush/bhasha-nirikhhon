declare var process : any;

var part_name = process.argv[2];
var num_pins = parseInt(process.argv[3]);
var pin_spacing : number = parseFloat(process.argv[4]);

var pad_length = pin_spacing * 1.5;
var pad_width = pin_spacing / 2;

var kicad_mod = "";
kicad_mod += "(module \"" + part_name + "\" (layer F.Cu) (tedit 54E7ACBD)\n";
kicad_mod += "(fp_text reference REF** (at 0 4) (layer F.SilkS)\n";
kicad_mod += "  (effects (font (size 1.5 1.5) (thickness 0.15)))\n";
kicad_mod += ")\n";
kicad_mod += "(fp_text value \"" + part_name + "\" (at 0 -4) (layer F.Fab)\n";
kicad_mod += "  (effects (font (size 1.5 1.5) (thickness 0.15)))\n";
kicad_mod += ")\n";

var y_coord : number = pad_length / 2 - pad_length/8;
var x_coord : number = -pin_spacing * num_pins/2 - pin_spacing/2;

for (var n = 1; n <= num_pins; n++)
{         
    y_coord *= -1;
    x_coord += pin_spacing;

    kicad_mod += "(pad " + n + " smd rect (at " + x_coord + " " + y_coord + ") (size " + pad_width + " " + pad_length + ") (layers F.Cu F.Paste F.Mask))\n";
} 
kicad_mod += ")\n";

console.log(kicad_mod);