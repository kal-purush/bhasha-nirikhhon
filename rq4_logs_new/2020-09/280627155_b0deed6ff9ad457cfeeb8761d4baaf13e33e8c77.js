MathJax = {
  /* Mathjax has difficulty recognizing `$$`, which Kramdown assumes. */
  options: {
    renderActions: {
      find: [
        10,
        function (doc) {
          for (const node of document.querySelectorAll(
            'script[type^="math/tex"]'
          )) {
            const display = !!node.type.match(/; *mode=display/);
            const math = new doc.options.MathItem(
              node.textContent,
              doc.inputJax[0],
              display
            );
            const text = document.createTextNode("");
            node.parentNode.replaceChild(text, node);
            math.start = { node: text, delim: "", n: 0 };
            math.end = { node: text, delim: "", n: 0 };
            doc.math.push(math);
          }
        },
        "",
      ],
    },
  },

  loader: {
    load: ['[tex]/ams'],
    load: ['[tex]/unicode']
  },

  svg: {
    displayAlign: "left",
    displayIndent: "3em"
  },

  /* * * * * * * * * * * * * * * * * * * * * * * */
  /* * * * * * * * * * * * * * * * * * * * * * * */
  /* * * * * * * * * * * * * * * * * * * * * * * */

  tex: {
    packages: {
      '[+]': ['ams'],
      '[+]': ['unicode']
    },

    inlineMath: ['$$', '$$'],
    displayMath: ['\\[', '\\]'],

    /* Define LaTeX macros, which will be expanded before TeX
    is called. Beware that `\` has to be escaped in JS. */
    macros: {

      /* * * * * * * * * * * * * * * * * * * * * * * */
      /* H: helper;
      N: number, L: Latin, G: Greek, C: Cyrillic
      T: text, K: keyword, V: variant;
      A: arithmetic, R: relation, P: puctuation, E: empty;
      X: letter-like symbols, Y: non-letter symbols, Z: geometric symbols;
      D: diacritics, I: infra, S: supra, U: under, O: over, B: bracket;
      Q: quadratic, F: fraction, W: while, M: matrix, J: join */

      /* * * * * * * * * * * * * * * * * * * * * * * */
      /* Group 0: H */

      // helper functions
      HDU: ["\\smash {\\underset {\\raise {8mu} {#1}} {#2}}", 2], // diacritics under
      HDO: ["\\smash {\\overset {\\raise {-1mu} {#1}} {#2}}", 2], // diacritics over

      HPt: ["\\phantom {\\rule[#1mu]{#2mu}{#3mu}}", 3], // phantom
      HGR: ["{\\large \\unicode [Source Serif Pro] {#1}}", 1], // glyph roman
      HGG: ["{\\large \\unicode [Source Sans Pro] {#1}}", 1], // glyph gothic
      HGRB: ["{\\large \\unicode [Alegreya] {#1}}", 1], // glyph roman, black
      HGGB: ["{\\large \\unicode [Alegreya Sans] {#1}}", 1], // glyph gothic, black
      HGJ: ["{\\large \\unicode [Noto Serif KR] {#1}}", 1], // glyph gothic, black
      HTR: ["\\HPt {#1} {#2} {#3} \\smash {\\HGR {#4}} \\HPt {#1} {#2} {#3}", 4], // typeset roman
      HTG: ["\\HPt {#1} {#2} {#3} \\smash {\\HGG {#4}} \\HPt {#1} {#2} {#3}", 4], // typeset gothic
      HTRB: ["\\HPt {#1} {#2} {#3} \\smash {\\HGRB {#4}} \\HPt {#1} {#2} {#3}", 4], // typeset roman, black
      HTGB: ["\\HPt {#1} {#2} {#3} \\smash {\\HGGB {#4}} \\HPt {#1} {#2} {#3}", 4], // typeset gothic, black
      HTJ: ["\\HPt {#1} {#2} {#3} \\smash {\\HGJ {#4}} \\HPt {#1} {#2} {#3}", 4], // typeset gothic, black
      HTK: ["\\hspace{2mu} #1 \\hspace{2mu}", 1], //typeset keyword

      /* * * * * * * * * * * * * * * * * * * * * * * */
      /* Group 1: N, L, G, C, T, K */


      // number (N)
      NZ: "\\HTR {0} {1} {11} {x0030}", // zerum
      NM: "\\HTR {0} {1} {11} {x0031}", // mónos
      ND: "\\HTR {0} {1} {11} {x0032}", // duo
      NT: "\\HTR {0} {1} {11} {x0033}", // trēs
      NQ: "\\HTR {0} {1} {11} {x0034}", // quattour
      NP: "\\HTR {0} {1} {11} {x0035}", // pénte
      NH: "\\HTR {0} {1} {11} {x0036}", // héx
      NS: "\\HTR {0} {1} {11} {x0037}", // septem
      NO: "\\HTR {0} {1} {11} {x0038}", // octō
      NN: "\\HTR {0} {1} {11} {x0039}", // novem

      // Latin (L)
      LA: "\\HTR {0} {1} {11} {x0041}",
      LB: "\\HTR {0} {1} {11} {x0042}",
      LC: "\\HTR {0} {1} {11} {x0043}",
      LD: "\\HTR {0} {1} {11} {x0044}",
      LE: "\\HTR {0} {1} {11} {x0045}",
      LF: "\\HTR {0} {1} {11} {x0046}",
      LG: "\\HTR {0} {1} {11} {x0047}",
      LH: "\\HTR {0} {1} {11} {x0048}",
      LI: "\\HTR {0} {1} {11} {x0049}",
      LK: "\\HTR {0} {1} {11} {x004B}",
      LL: "\\HTR {0} {1} {11} {x004C}",
      LM: "\\HTR {0} {1} {11} {x004D}",
      LN: "\\HTR {0} {1} {11} {x004E}",
      LO: "\\HTR {0} {1} {11} {x004F}",
      LP: "\\HTR {0} {1} {11} {x0050}",
      LR: "\\HTR {0} {1} {11} {x0052}",
      LS: "\\HTR {0} {1} {11} {x0053}",
      LT: "\\HTR {0} {1} {11} {x0054}",
      LU: "\\HTR {0} {1} {11} {x0055}",
      LV: "\\HTR {0} {1} {11} {x0056}",
      LW: "\\HTR {0} {1} {11} {x0057}",
      LX: "\\HTR {0} {1} {11} {x0058}",
      LY: "\\HTR {0} {1} {11} {x0059}",
      LZ: "\\HTR {0} {1} {11} {x005A}",

      LQ: "\\HTR {-2} {1} {13} {x0051}",
      LJ: "\\HTR {-2} {1} {13} {x004A}",

      La: "\\HTR {0} {1} {8} {x0061}",
      Lc: "\\HTR {0} {1} {8} {x0063}",
      Le: "\\HTR {0} {1} {8} {x0065}",
      Li: "\\HTR {0} {1} {8} {x0131}",
      Lm: "\\HTR {0} {1} {8} {x006D}",
      Ln: "\\HTR {0} {1} {8} {x006E}",
      Lo: "\\HTR {0} {1} {8} {x006F}",
      Lr: "\\HTR {0} {1} {8} {x0072}",
      Ls: "\\HTR {0} {1} {8} {x0073}",
      Lu: "\\HTR {0} {1} {8} {x0075}",
      Lv: "\\HTR {0} {1} {8} {x0076}",
      Lw: "\\HTR {0} {1} {8} {x0077}",
      Lx: "\\HTR {0} {1} {8} {x0078}",
      Lz: "\\HTR {0} {1} {8} {x007A}",

      Lb: "\\HTR {0} {1} {12} {x0062}",
      Ld: "\\HTR {0} {1} {12} {x0064}",
      Lf: "\\HTR {0} {1} {12} {x0066}",
      Lh: "\\HTR {0} {1} {12} {x0068}",
      Lk: "\\HTR {0} {1} {12} {x006B}",
      Ll: "\\HTR {0} {1} {12} {x006C}",

      Lt: "\\HTR {0} {1} {10} {x0074}",

      Lg: "\\HTR {-3} {1} {11} {x0067}",
      Lp: "\\HTR {-3} {1} {11} {x0070}",
      Lq: "\\HTR {-3} {1} {11} {x0071}",
      Lj: "\\HTR {-3} {1} {11} {x0237}",
      Ly: "\\HTR {-3} {1} {11} {x0079}",
      
      // Greek (G)

      GG: "\\HTR {0} {1} {11} {x0393}",
      GD: "\\HTR {0} {1} {11} {x0394}",
      GQ: "\\HTR {0} {1} {11} {x0398}",
      GL: "\\HTR {0} {1} {11} {x039B}",
      GC: "\\HTR {0} {1} {11} {x039E}",
      GP: "\\HTR {0} {1} {11} {x03A0}",
      GS: "\\HTR {0} {1} {11} {x03A3}",
      GU: "\\HTR {0} {1} {11} {x03A5}",
      GF: "\\HTR {0} {1} {11} {x03A6}",
      GY: "\\HTR {0} {1} {11} {x03A8}",
      GW: "\\HTR {0} {1} {11} {x03A9}",

      Ga: "\\HTR {0} {1} {8} {x03B1}",
      Gv: "\\HTR {0} {1} {8} {x03B5}",
      Gi: "\\HTR {0} {1} {8} {x03B9}",
      Gk: "\\HTR {0} {1} {8} {x03BA}",
      Gm: "\\HTR {0} {1} {8} {x03BC}",
      Gn: "\\HTR {0} {1} {8} {x03BD}",
      Gp: "\\HTR {0} {1} {8} {x03C0}",
      Gs: "\\HTR {0} {1} {8} {x03C3}",
      Gt: "\\HTR {0} {1} {8} {x03C4}",
      Gu: "\\HTR {0} {1} {8} {x03C5}",
      Gw: "\\HTR {0} {1} {8} {x03C9}",

      Gb: "\\HTR {-3} {1} {15} {x03B2}",
      Gz: "\\HTR {-3} {1} {15} {x03B6}",
      Gc: "\\HTR {-3} {1} {15} {x03BE}",

      Gf: "\\HTR {-3} {1} {13} {x03C6}",
      Gy: "\\HTR {-3} {1} {13} {x03C8}",

      Gd: "\\HTR {0} {1} {12} {x03B4}",
      Gq: "\\HTR {0} {1} {12} {x03B8}",
      Gl: "\\HTR {0} {1} {12} {x03BB}",

      Gg: "\\HTR {-3} {1} {11} {x03B3}",
      Gh: "\\HTR {-3} {1} {11} {x03B7}",
      Gr: "\\HTR {-3} {1} {11} {x03C1}",
      Gx: "\\HTR {-3} {1} {11} {x03C7}",

      // C

      // \\large
      //БДЄЖЗИЛЦЧЏШЩЭЯ
      //бдєжзилцчџшщэя


      /* * * * * * * * * * * * * * * * * * * * * * * */
      /* Group 2: A, R, P, E */

      // arithmatic (A)
      AA: "+", // addition
      AS: "-", // subtraction
      AM: "\\cdot", // multiplication
      AD: "/", // division

      RE: "=", //  equal
      REN: "\\neq", //  equal
      RG: ">", // greater
      RL: "<", //  less
      REG: "\\geq",
      REL: "\\leq",
      RS: "\\sim",
      RSE: "\\simeq",
      RSS: "\\approx",
      RS: "\\succ",
      RP: "\\prec",
      RES: "\\succeq",
      REP: "\\preceq",
      RSN: "\\nsucc",
      RPN: "\\nprec",
      RM: "\\mid",
      RMN: "\\nmid",
      RD: ":=", // definition

      RAL: "\\leftarrow",
      RAR: "\\rightarrow",
      RALR: "\\leftrightarrow",
      RALT: "\\Leftarrow", // thick
      RART: "\\Rightarrow",
      RALRT: "\\Leftrightarrow",
      RAD: "\\downarrow",
      RAU: "\\uparrow",
      RADU: "\\updownarrow",
      RADT: "\\Downarrow",
      RAUT: "\\Uparrow",
      RADUT: "\\Updownarrow",
      RALH: "\\leftharpoonup", // harpoon
      RARH: "\\rightharpoonup",
      RALRH: "\\leftrightharpoonup",

      PC: ",", // comma
      PP: ".", // period
      PR: ":", // ratio
      PS: ";", // semicolon
      PE: "!", // exclamation
      PQ: "?", // question
      PDC: "\\cdots",
      PDL: "\\ldots",
      PDV: "\\vdots",
      PDD: "\\ddots",

      EA: "\\hspace {0.20em}",
      EB: "\\hspace {0.40em}",
      EC: "\\hspace {0.60em}",
      ED: "\\hspace {0.80em}",
      EE: "\\hspace {1.00em}",
      EF: "\\hspace {1.20em}",
      EG: "\\hspace {1.40em}",
      EH: "\\hspace {1.60em}",
      EI: "\\hspace {1.80em}",

      // text (T)
      TA: "\\HTG {0} {0} {11} {x0041}",
      TB: "\\HTG {0} {0} {11} {x0042}",
      TC: "\\HTG {0} {0} {11} {x0043}",
      TD: "\\HTG {0} {0} {11} {x0044}",
      TE: "\\HTG {0} {0} {11} {x0045}",
      TF: "\\HTG {0} {0} {11} {x0046}",
      TG: "\\HTG {0} {0} {11} {x0047}",
      TH: "\\HTG {0} {0} {11} {x0048}",
      TI: "\\HTG {0} {0} {11} {x0049}",
      TJ: "\\HTG {0} {0} {11} {x004A}",
      TK: "\\HTG {0} {0} {11} {x004B}",
      TL: "\\HTG {0} {0} {11} {x004C}",
      TM: "\\HTG {0} {0} {11} {x004D}",
      TN: "\\HTG {0} {0} {11} {x004E}",
      TO: "\\HTG {0} {0} {11} {x004F}",
      TP: "\\HTG {0} {0} {11} {x0050}",
      TR: "\\HTG {0} {0} {11} {x0052}",
      TS: "\\HTG {0} {0} {11} {x0053}",
      TT: "\\HTG {0} {0} {11} {x0054}",
      TU: "\\HTG {0} {0} {11} {x0055}",
      TV: "\\HTG {0} {0} {11} {x0056}",
      TW: "\\HTG {0} {0} {11} {x0057}",
      TX: "\\HTG {0} {0} {11} {x0058}",
      TY: "\\HTG {0} {0} {11} {x0059}",
      TZ: "\\HTG {0} {0} {11} {x005A}",

      TQ: "\\HTG {-2} {0} {13} {x0051}",

      Ta: "\\HTG {0} {0} {8} {x0061}",
      Tc: "\\HTG {0} {0} {8} {x0063}",
      Te: "\\HTG {0} {0} {8} {x0065}",
      Tm: "\\HTG {0} {0} {8} {x006D}",
      Tn: "\\HTG {0} {0} {8} {x006E}",
      To: "\\HTG {0} {0} {8} {x006F}",
      Tr: "\\HTG {0} {0} {8} {x0072}",
      Ts: "\\HTG {0} {0} {8} {x0073}",
      Tu: "\\HTG {0} {0} {8} {x0075}",
      Tv: "\\HTG {0} {0} {8} {x0076}",
      Tw: "\\HTG {0} {0} {8} {x0077}",
      Tx: "\\HTG {0} {0} {8} {x0078}",
      Tz: "\\HTG {0} {0} {8} {x007A}",
 
      Tb: "\\HTG {0} {0} {12} {x0062}",
      Td: "\\HTG {0} {0} {12} {x0064}",
      Tf: "\\HTG {0} {0} {12} {x0066}",
      Th: "\\HTG {0} {0} {12} {x0068}",
      Ti: "\\HTG {0} {0} {12} {x0069}",
      Tk: "\\HTG {0} {0} {12} {x006B}",
      Tl: "\\HTG {0} {0} {12} {x006C}",

      Tt: "\\HTG {0} {0} {10} {x0074}",

      Tg: "\\HTG {-3} {0} {11} {x0067}",
      Tp: "\\HTG {-3} {0} {11} {x0070}",
      Tq: "\\HTG {-3} {0} {11} {x0071}",
      Ty: "\\HTG {-3} {0} {11} {x0079}",

      Tj: "\\HTG {-3} {0} {15} {x006A}",

      // keywords (K)
      KSin: "\\HTK {\\Ts \\Ti \\Tn}",
      KCos: "\\HTK {\\Tc \\To \\Ts}",
      KTan: "\\HTK {\\Tt \\Ta \\Tn}",
      KSinh: "\\HTK {\\Ts \\Ti \\Tn \\Th}",
      KCosh: "\\HTK {\\Tc \\To \\Ts \\Th}",
      KTanh: "\\HTK {\\Tt \\Ta \\Tn \\Th}",
      KLog: "\\HTK {\\Tl \\To \\Tg}",
      KExp: "\\HTK {\\Te \\Tx \\Tp}",
      KRe: "\\HTK {\\Tr \\Te}",
      KIm: "\\HTK {\\Ti \\Tm}",
      KSupp: "\\HTK {\\Ts \\Tu \\Tp \\Tp}",

      KMax: "\\HTK {\\Tm \\Ta \\Tx}",
      KMin: "\\HTK {\\Tm \\Ti \\Tn}",
      KSup: "\\HTK {\\Ts \\Tu \\Tp}",
      KInf: "\\HTK {\\Ti \\Tn \\Tf}",
      KLim: "\\HTK {\\Tl \\Ti \\Tm}",
      KLimsup: "\\HTK {\\Tl \\Ti \\Tm \\Ts \\Tu \\Tp}",
      KLiminf: "\\HTK {\\Tl \\Ti \\Tm \\Ti \\Tn \\Tf}",

      KKer: "\\HTK {\\Tk \\Te \\Tr}",
      KImg: "\\HTK {\\Ti \\Tm \\Tg}",
      KHom: "\\HTK {\\Th \\To \\Tm}",
      KDim: "\\HTK {\\Td \\Ti \\Tm}",
      KDet: "\\HTK {\\Td \\Te \\Tt}",
      KMod: "\\HTK {\\Tm \\To \\Td}",
      KGcd: "\\HTK {\\Tg \\Tc \\Td}",

      /* * * * * * * * * * * * * * * * * * * * * * * */
      /* Group 2: X, Y, Z */

      XLo: "\\varnothing",

      YT: "\\intercal",


      ZI: "\\infty", // infinity
      ZSW: "\\square",
      ZSB: "\\blacksquare",
      ZTU: "\\triangle",
      ZTD: "\\triangledown",
      ZTL: "\\triangleleft",
      ZTR: "\\triangleright",
      ZTBU: "\\blacktriangle",
      ZTBD: "\\blacktriangledown",
      ZTBL: "\\blacktriangleleft",
      ZTBR: "\\blacktriangleright",
      ZM: "\\bigstar", // mullet
      ZCD: "\\diamondsuit", // color
      ZCC: "\\clubsuit",
      ZCH: "\\heartsuit",
      ZCS: "\\spadesuit",
      ZObl: "\\dagger", // obelus
      ZDes: "\\ddagger", // Diesis

      /* Group 3: D, I, S, U, O, B */
      DUT: ["\\HDU {\\tiny \\boldsymbol {\\thicksim}} {#1} \\vphantom {#1}", 1], // tilde,
      DUM: ["\\HDU {\\small \\textbf {-}} {#1} \\vphantom {#1}", 1], // macron,
      DUH: ["\\HDU {\\tiny \\boldsymbol {\\vee}} {#1} \\vphantom {#1}", 1], // hachek,
      DUC: ["\\HDU {\\tiny \\boldsymbol {\\wedge}} {#1} \\vphantom {#1}", 1], // circumflex,
      DUR: ["\\HDU {\\tiny \\boldsymbol {\\circ}} {#1} \\vphantom {#1}", 1], // ring,
      DUD: ["\\HDU {\\tiny \\bullet} {#1} \\vphantom {#1}", 1], // dot
      DUP: ["\\HDU {\\small \\textbf{,}} {#1} \\vphantom {#1}", 1], // psili,

      DOT: ["\\HDO {\\tiny \\boldsymbol {\\thicksim}} {#1} \\vphantom {#1}", 1],
      DOM: ["\\HDO {\\small \\textbf {-}} {#1} \\vphantom {#1}", 1],
      DOH: ["\\HDO {\\tiny \\boldsymbol {\\vee}} {#1} \\vphantom {#1}", 1],
      DOC: ["\\HDO {\\tiny \\boldsymbol {\\wedge}} {#1} \\vphantom {#1}", 1],
      DOR: ["\\HDO {\\tiny \\boldsymbol {\\circ}} {#1} \\vphantom {#1}", 1],
      DOD: ["\\HDO {\\tiny \\bullet} {#1} \\vphantom {#1}", 1],
      DOP: ["\\HDO {\\small \\textbf{,}} {#1} \\vphantom {#1}", 1],

      I: ["{#1} _{#2}", 2],
      S: ["{#1} ^{#2}", 2],
      IS: ["{#1} _{#2} ^{#3}", 3],
      IE: ["{\\phantom {\\rule[0]{1}{11}}} _{#2}", 1], // empty
      SE: ["{\\phantom {\\rule[0]{1}{11}}} ^{#2}", 1],
      ISE: ["{\\phantom {\\rule[0]{1}{11}}} _{#1} ^{#2}", 2],
      
      F: ["\\frac {#1} {#2}", 2], // fraction
      Q: ["\\sqrt {#1}", 1], // quadratic


      BR: ["\\left( #1 \\right)", 1],
      BRA: ["( #1 )", 1],
      BRB: ["\\big( #1 \\big)", 1],
      BRC: ["\\Big( #1 \\Big)", 1],
      BRD: ["\\bigg( #1 \\bigg)", 1],
      BRE: ["\\Bigg( #1 \\Bigg)", 1],
      BS: ["\\left( #1 \\right)", 1],
      BSA: ["[ #1 ]", 1],
      BSB: ["\\big[ #1 \\big]", 1],
      BSC: ["\\Big[ #1 \\Big]", 1],
      BSD: ["\\bigg[ #1 \\bigg]", 1],
      BSE: ["\\Bigg[ #1 \\Bigg]", 1],
      BC: ["\\left\\{ #1 \right\\}", 1],
      BCA: ["\\{ #1 \\}", 1],
      BCB: ["\\big\\{ #1 \\big\\}", 1],
      BCC: ["\\Big\\{ #1 \\Big\\}", 1],
      BCD: ["\\bigg\\{ #1 \\bigg\\}", 1],
      BCE: ["\\Bigg\\{ #1 \\Bigg\\}", 1],

      /* Group 4: Q, F, W, M */

      // while-like constructions (W)
      WI: "\\int",
      WID: ["\\int \\!\\! {\\vphantom {\\int}}^{#1}", 1],
      WII: ["\\int\\limits_{#1}", 1],
      WIID: ["\\int\\limits_{#1} \\!\\! {\\vphantom {\\int}}^{#2}", 2],
      WIIS: ["\\int\\limits_{#1}^{#2}", 2],
      WIISD: ["\\int\\limits_{#1}^{#2} \\!\\! {\\vphantom {\\int}}^{#3}", 3],

      WP: "\\bigoplus",
      WPD: ["\\bigoplus \\!\\! {\\vphantom {\\int}}^{#1}", 1],
      WPI: ["\\bigoplus\\limits_{#1}", 1],
      WPID: ["\\bigoplus\\limits_{#1} \\!\\! {\\vphantom {\\int}}^{#2}", 2],
      WPIS: ["\\bigoplus\\limits_{#1}^{#2}", 2],
      WPISD: ["\\bigoplus\\limits_{#1}^{#2} \\!\\! {\\vphantom {\\int}}^{#3}", 3],

      WM: "\\bigodot",
      WMD: ["\\bigodot \\!\\! {\\vphantom {\\int}}^{#1}", 1],
      WMI: ["\\bigodot\\limits_{#1}", 1],
      WMID: ["\\bigodot\\limits_{#1} \\!\\! {\\vphantom {\\int}}^{#2}", 2],
      WMIS: ["\\bigodot\\limits_{#1}^{#2}", 2],
      WMISD: ["\\bigodot\\limits_{#1}^{#2} \\!\\! {\\vphantom {\\int}}^{#3}", 3],


      // matrix (M)
      MCA: ["\\begin {bmatrix} #1 \\end {bmatrix}", 1], // column
      MCB: ["\\begin {bmatrix} #1 \\\\ #2 \\end {bmatrix}", 2],
      MCC: ["\\begin {bmatrix} #1 \\\\ #2 \\\\ #3  \\end {bmatrix}", 3],

      MRA: ["#1", 1], // row
      MRB: ["#1 & #2", 2],
      MRC: ["#1 & #2 & #3", 3],

      JHA: "\\HTJ {0} {1} {11} {x3042}",
      JKA: "\\HTJ {0} {1} {11} {x9F20}",


      // Japanese (J)
      //    Hirakana (JH)
      //    Kanji (JK)
      //       Zodiac (Z): 鼠牛虎兎龍蛇馬羊猿鶏犬猪
      //       Stem (S): 甲乙丙丁戊己庚辛壬癸
      //       Branch (B): 子丑寅卯辰巳午未申酉戌亥 
      //       Trigramme (T): 乾兌離震巽坎艮坤


/* Deprecated

      // letter-like (Y, Z)
      Ld: "\\partial",
      LT: "\\intercal",
      Lp: "\\pi",

      % # @ &
      Đ Ґ Ђ Є Ћ Џ
      $ € £ ¥ ¢

*/

    },
  },
};



