/** 
*** Aluminium facts bot to bring wonderful aluminium facts to everyone's life
*** Author - Steve Shipsey
**/

import { Robot } from "tsbot";

let facts = ["Trippy: About 75 percent of all aluminium ever made is still in use, thanks to recycling, according to the aluminium Association.",
"That can of Coke may not have been off the shelf for long. According to the aluminium Association, an aluminium can takes as little as 60 days to return as a new can after recycling.",
"The top of the Washington Monument is capped with an 8.9-inch (22.6 centimeters) aluminium pyramid. The aluminium cap initially served as the apex of the monument's lightning rod, though it had to be augmented with copper rods when it became clear that the cap alone could not prevent damage.",
"Recycle! Recycling aluminium takes only 5 percent of the energy needed to extract new aluminium from ore, according to the EPA. As of 2012, about 55 percent of aluminium drink cans made it into the recycling bin.",
"Don't try this at home (unless you have a fume hood). aluminium powder plus iodine plus a few drops of water create quite a display: Clouds of toxic purple iodine vapor and then sudden flame. The reaction is a demonstration of how reactive aluminium can really be.",
"A single Boeing-747 contains 147,000 pounds (more than 66,000 kilograms) of aluminium, according to Chemicool.",
"aluminium is plentiful: In 2012, according to the U.S. Environmental Protection Agency (EPA), 1.9 million tons of aluminium were produced for containers and packaging alone. Another 1.7 million tons went toward appliances, vehicle parts and other durable goods.",
"Boiling Point: 4,566 degrees F (2,519 degrees C)",
"Melting Point: 1,220.58 degrees Fahrenheit (660.32 degrees Celsius)",
"Density:  2.70 grams per cubic centimeter",
"Aluminium is present in more than 270 minerals.",
"It is the most abundant mineral on Earth after oxygen and silicon.",
"It is also the most abundant metal found naturally on Earth.",
"Aluminium is globally the most used metal that does not contain iron.",
"Aluminium is almost always used as an alloy, even if the aluminium content is as high as 99%.",
"The most commonly used elements to combine with aluminium to create an alloy are zinc, copper, silicon, magnesium, and manganese.",
"Aluminium salts do not serve any known purpose in plant or animal life.",
"It is, however, not highly toxic to living organisms in small amounts.",
"Aluminium reflects about 92% of visible light.",
"It reflects about 98% of infrared rays.",
"Its density and stiffness are about a third of the density and stiffness of steel.",
"There are many recognized isotopes of aluminium, but only two are found in nature.",
"Because of aluminium's high likelihood of binding with oxygen, pure aluminium is almost never found in nature.",
"Aluminium's silicates or oxides are more likely to be found naturally.",
"Aluminium is extremely difficult to isolate from minerals because it is extremely reactive.",
"The ores that contain aluminium have a very high melting point, making extraction problematic.",
"Australia is the leading producer of the world's aluminium.",
"Aluminium is potentially fully recyclable.",
"Recycling aluminium requires only five percent of the energy that extracting it from ore requires.",
"The byproduct of aluminium production and recycling is called white dross. White dross can be highly combustible, but serves purposes in concrete and asphalt production.",
"Aluminium manufacturing takes a lot of energy – 17.4 megawatt hours of electrical energy to produce one metric ton of aluminium; that’s three times more energy than is needed to make a metric ton of steel.",
"Aluminium is a great metal to recycle. Recycling uses only 5% of the energy needed to produce aluminium from its ore, bauxite.",
"Aluminium does not stick to magnets under normal conditions.",
"There is more aluminium in the Earth’s crust than any other metal. At about 8 percent, aluminium is the third most abundant element in our planet’s crust, behind oxygen and silicon.",
"Despite its high abundance, in the 1850s aluminium was more valuable than gold. In 1852 aluminium was priced at $1200 per kg and gold was $664 per kg.",
"Aluminium prices illustrate the perils of financial speculation: in 1854 Saint-Claire Deville found a way of replacing potassium with much cheaper sodium in the reaction to isolate aluminium. By 1859, aluminium was priced at $37 per kg; its price had dropped 97% in just five years.",
"Where the previous item highlights the perils of speculation, this item highlights one of the triumphs of chemistry: the Hall-Heroult electrolytic process was discovered in 1886. By 1895, aluminium’s price had dropped to just $1.20 per kg.",
"Ruby gemstones are mainly aluminium oxide in which a small number of the aluminium ions have been replaced by chromium ions.",
"Aluminium is made in the nuclear fires of heavy stars when a proton adds to magnesium. (Magnesium is itself made in stars by nuclear fusion of two carbons.)"];

let fact = (robot: Robot) => {
    robot.hear(/alumin(i)?um( me)?/i, (res) => {
        res.send(`${res.random(facts)}`);
    });
};