const OpenAI = require("openai");

const openai = new OpenAI({ apiKey: `${process.env.OPENAI_API_KEY}` });

async function LabelSearchAPI(base64Image) {
  const stream = await openai.chat.completions.create({
    model: "gpt-4o",
    messages: [
      {
        role: "user",
        content: [
          {
            type: "text",
            text: `I'll give you the information of the laundry symbols across a total of 95 rows below. Each row consists of three items and items are separated by a tab. Each item is "laundry symbol's number", "laundry symbol's description", and "shape of the laundry symbol". Now, please answer the laundry symbol number that best matches the image I provided to you. The provided image has washing symbols which are different from each other. If the provided image has multiple laundry symbols, please answer the "laundry symbol's number" of each of them.  If you're not sure of your answer, do not answer anything. Answer with only numbers and answer only one number per line. Never give unnecessary explanations. Don't say that you understand. 
1	Machine dryable.	A circle inscribed in a square
2	Not machine dryable.	A circle inscribed in a square with an X mark overlaying it
3	Machine dry at low temperature.    A circle inscribed in a square with a dot at the center
4	Machine dry at medium temperature.	  A circle inscribed in a square with two dots at the center
5	Machine dry at high temperature.	A circle inscribed in a square with three dots at the center
6	Dry in the shade.	A square with two positive oblique lines at the top left corner
7	Dry in the shade.	A sun shaped symbol with multiple positive oblique lines at the top left corner
8	Hang to dry.	A vertically long bar inside a square
9	Hang to dry in the shade.		A vertically long bar inside a square with a positive oblique line at the top left corner
10	Lay flat to dry.		A horizontally long bar inside a square
11	Lay flat to dry in the shade.		A horizontally long bar inside a square with a positive oblique line at the top left corner
12	Line dry while wet.	Three vertically long bars inside a square
13	Hang to dry.		A square with a downwardly convex curve at the upper corner
14	Hang to dry in the shade.		A square with a downwardly convex curve at the upper corner and two positive oblique lines at the top left corner
15	Hang on a hanger to dry.		A sun shaped symbol which written '옷걸이' at the center
16	Hang on a hanger to dry in the shade.	A sun shaped symbol which written '옷걸이' at the center and multiple positive oblique lines at the top left corner
17	Lay flat to dry.	A sun shaped symbol which written '뉘어서' at the center
18	Lay flat to dry in the shade.		A sun shaped symbol which written '뉘어서' at the center and multiple positive oblique lines at the top left corner
19	Gently wring out.	Three distorted circles attached together which written '약', '하', '게' inside each of them
20	Do not wring.	Three distorted circles attached together with X mark overlaying it
21	Machine washable.		A bowl shaped symbol which has a wavy line inside of it
22	Hand wash.	A bowl shaped symbol which has a wavy line and a hand-shaped symbol inside of it
23	Wash gently.	A bowl shaped symbol which has a wavy line inside of it and has a horizontal line below it
24	Wash very gently.	A bowl shaped symbol which has a wavy line inside of it and has two horizontal lines below it
25	Not machine washable.		A bowl shaped symbol which has a wavy line inside of it and X mark overlaying it
26	Wash at 30°C.	A bowl shaped symbol which has a wavy line inside of it and written '30°' inside of it
27	Wash gently at 30°C.	A bowl shaped symbol which has a wavy line inside of it and written '30°' inside of it and has a horizontal line below it
28	Wash at 40°C.		A bowl shaped symbol which has a wavy line inside of it and written '40°' inside of it
29	Wash at 50°C.		A bowl shaped symbol which has a wavy line inside of it and written '50°' inside of it
30	Wash at 60°C.		A bowl shaped symbol which has a wavy line inside of it and written '60°' inside of it
31	Wash at 70°C.		A bowl shaped symbol which has a wavy line inside of it and written '70°' inside of it
32	Wash at 80°C.		A bowl shaped symbol which has a wavy line inside of it and written '80°' inside of it
33	Wash at 90°C.		A bowl shaped symbol which has a wavy line inside of it and written '90°' inside of it
34	Wash at 95°C (Boil safe).		A bowl shaped symbol which has a wavy line inside of it and written '95°' inside of it
35	Wash at 30°C.		A bowl shaped symbol which has a wavy line inside of it and has a dot in the center of it
36	Wash at 40°C.		A bowl shaped symbol which has a wavy line inside of it and has two dots in the center of it
37	Wash at 50°C.		A bowl shaped symbol which has a wavy line inside of it and has three dots in the center of it
38	Wash at 60°C.		A bowl shaped symbol which has a wavy line inside of it and has four dots in the center of it
39	Wash at 70°C.		A bowl shaped symbol which has a wavy line inside of it and has five dots in the center of it
40	Wash at 95°C (Boil safe).		A bowl shaped symbol which has a wavy line inside of it and has six dots in the center of it
41	Hand wash.		A bowl shaped symbol which has a wavy line inside of it and written '손세탁' on a wavy line
42	Hand wash with a neutral detergent at a water temperature of 30˚C.	A bowl shaped symbol which has a wavy line inside of it and written '손세탁' on a wavy line and written '30°C 중성' below a wavy line
43	Wash at a water temperature of 30˚C.		A rectangular which has a horizontal line at the top of it and written '30°C' at the center of it
44	Wash gently at a water temperature of 30˚C.	A rectangular which has a horizontal line at the top of it and written '약 30°C' at the center of it
45	Wash gently with a neutral detergent at a water temperature of 30˚C.	A rectangular which has a horizontal line at the top of it and written '약 30°C 중성' at the center of it
46	Wash at a water temperature of 40˚C.	A rectangular which has a horizontal line at the top of it and written '40°C' at the center of it
47	Wash gently at a water temperature of 40˚C.	A rectangular which has a horizontal line at the top of it and written '약 40°C' at the center of it
48	Wash at a water temperature of 50˚C.	A rectangular which has a horizontal line at the top of it and written '50°C' at the center of it
49	Wash at a water temperature of 60˚C.	A rectangular which has a horizontal line at the top of it and written '60°C' at the center of it
50	Wash at a water temperature of 70˚C.	A rectangular which has a horizontal line at the top of it and written '70°C' at the center of it
51	Wash at a water temperature of 80˚C.	A rectangular which has a horizontal line at the top of it and written '80°C' at the center of it
52	Wash at a water temperature of 90˚C.	A rectangular which has a horizontal line at the top of it and written '90°C' at the center of it
53	Wash at a water temperature of 95˚C. (Can be boiled.)	A rectangular which has a horizontal line at the top of it and written '95°C' at the center of it
54	Ironable.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner
55	Non-ironable.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner and X mark overlaying it
56	Steam ironing is possible.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner and a positive oblique line and a negative oblique line at the bottom of the center
57	Steam ironing is not possible.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner and a positive oblique line and a negative oblique line at the bottom of the center and X mark overlaying it
58	Iron at a maximum temperature of 100˚C.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner and a dot inside of it
59	Iron at a maximum temperature of 150˚C.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner and two dots inside of it
60	Iron at a maximum temperature of 200˚C.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner and three dots inside of it
61	Iron at 80-120˚C.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner and written '80~120°C' inside of it
62	Iron at 140-160˚C.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner and written '140~160°C' inside of it
63	Iron at 180-210˚C.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner and written '180~210°C' inside of it
64	Iron with a cloth over the fabric.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner and a wavy line below it
65	Iron at 80-120˚C with a cloth over the fabric.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner and written '80~120°C' inside of it and a wavy line below it
66	Iron at 140-160˚C with a cloth over the fabric.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner and written '140~160°C' inside of it and a wavy line below it
67	Iron at 180-210˚C with a cloth over the fabric.	A horizontal rectangle connected with "a rotated L-shape that is flipped and rotated 90 degrees to the left" at the top right corner and written '180~210°C' inside of it and a wavy line below it
68	Do not use bleach.	A triangle with an X mark overlaying it
69	Use non-chlorine bleach.	A triangle with two diagonal lines inside
70	Use chlorine bleach.	A triangle with 'CL' written inside
71	All bleaches can be used.	A blank triangle
72	Do not use bleach.	A triangle filled with black and an X mark overlaying it
73	Use oxygen bleach.	A triangle with '산소 표백' written inside
74	Do not use oxygen bleach.	A triangle with '산소 표백' inside and an X mark overlaying it
75	Use chlorine bleach.	A triangle with '염소 표백' written inside
76	Do not use chlorine bleach.	A triangle with '염소 표백' written inside and an X mark overlaying it
77	Use oxygen and chlorine bleaches.	A triangle with '염소, 산소 표백' written inside
78	Do not use oxygen or chlorine bleaches.	A triangle with '염소, 산소 표백' written inside and an X mark overlaying it
79	Dry cleaning is possible.	A blank circle.
80	Dry clean at a low temperature.	A circle with a negative oblique line at the bottom right corner.
81	Dry clean in a short period of time.	A circle with a positive oblique line at the bottom left corner.
82	Dry clean with minimal moisture.	A circle with a positive oblique line at the top left corner.
83	Dry clean without steam.	A circle with a negative oblique line at the top right corner.
84	Dry cleaning is possible.	A circle with a wavy line inside, and '드라이' written inside.
85	Dry clean at a professional service.	A circle with a wavy line inside, and '드라이' written inside, with a single line segment below the circle.
86	Dry cleaning is not possible.	A circle with a wavy line inside, and '드라이' written inside, with an X mark overlaying it.
87	Dry clean with petroleum-based solvents.	A circle with a wavy line inside, with '드라이' above the wavy line and '석유계' below the wavy line
88	Dry clean with all solvents.	A circle with an 'A' written inside.
89	Dry clean with hydrocarbon solvents.	A circle with an 'F' written inside.
90	Dry clean with perchloroethylene solvent.	A circle with a 'P' written inside.
91	Dry cleaning is not possible.	A circle with an X mark overlaying it.
92	Wet cleaning is not possible.	A circle filled with black and an X mark overlaying it.
93	Wet cleaning is possible.	A circle with a 'W' written inside.
94	Wet clean gently.	A circle with a 'W' written inside, and a single line segment below the circle.
95	Wet cleaning is not possible.	A circle with a 'W' written inside, with an X mark overlaying it.
`,
          },
          {
            type: "image_url",
            image_url: {
              url: `data:image/jpeg;base64,${base64Image}`,
              detail: "low",
            },
          },
        ],
      },
    ],
    stream: true,
    max_tokens: 500,
  });

  const listing = [];

  for await (const chunk of stream) {
    const temp = chunk.choices[0]?.delta?.content || "";
    if (temp == "" || temp == "\n") {
      continue;
    }
    listing.push(temp);
  }

  return listing;
}