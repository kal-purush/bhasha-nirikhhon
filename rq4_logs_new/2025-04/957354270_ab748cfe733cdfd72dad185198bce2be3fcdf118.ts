export interface Question {
  question: string;
  options: string[];
  answer: string;
}

export interface ExperimentQuestions {
  experiment: string;
  questions: Question[];
}

// Pre-test questions
export const preTestQuestions: Record<string, ExperimentQuestions> = {
  "1": {
    experiment: "IV-Characteristics",
    questions: [
      {
        question: "What does 'IV' stand for in IV-characteristics?",
        options: ["Initial Voltage", "Input Voltage", "Current-Voltage", "Inverse Voltage"],
        answer: "Current-Voltage"
      },
      {
        question: "What is the unit of current?",
        options: ["Ohm", "Volt", "Ampere", "Watt"],
        answer: "Ampere"
      },
      {
        question: "What is plotted in an IV curve?",
        options: ["Current vs Voltage", "Power vs Resistance", "Voltage vs Resistance", "Current vs Time"],
        answer: "Current vs Voltage"
      },
      {
        question: "What is the slope of an IV curve for a resistor?",
        options: ["Resistance", "Conductance", "Voltage", "Power"],
        answer: "Conductance"
      },
      {
        question: "Which law governs the IV characteristics of a resistor?",
        options: ["Newton's Law", "Ohm's Law", "Faraday's Law", "Lenz's Law"],
        answer: "Ohm's Law"
      }
    ]
  },
  "2": {
    experiment: "Numerical Aperture",
    questions: [
      {
        question: "Numerical Aperture is a measure of a fiber's...",
        options: ["Width", "Core material", "Light-gathering ability", "Wavelength"],
        answer: "Light-gathering ability"
      },
      {
        question: "Formula for NA is...",
        options: ["NA = sinθ", "NA = √(n₁² - n₂²)", "NA = n₁ + n₂", "NA = 1 / sinθ"],
        answer: "NA = √(n₁² - n₂²)"
      },
      {
        question: "Higher NA means...",
        options: ["Less light accepted", "More light accepted", "Less bending", "Smaller angle"],
        answer: "More light accepted"
      },
      {
        question: "NA depends on which part of fiber?",
        options: ["Cladding only", "Core and cladding", "Jacket", "All of the above"],
        answer: "Core and cladding"
      },
      {
        question: "NA is dimensionless. True or False?",
        options: ["True", "False"],
        answer: "True"
      }
    ]
  },
  "3": {
    experiment: "Wedge Shape",
    questions: [
      {
        question: "Wedge-shaped film is used to study...",
        options: ["Reflection", "Refraction", "Interference", "Diffraction"],
        answer: "Interference"
      },
      {
        question: "What shape does the air film between the plates form?",
        options: ["Circular", "Rectangular", "Wedge", "Triangular"],
        answer: "Wedge"
      },
      {
        question: "Fringes formed in wedge film are...",
        options: ["Circular", "Elliptical", "Straight and parallel", "Random"],
        answer: "Straight and parallel"
      },
      {
        question: "The thickness of air film at fringe location determines...",
        options: ["Fringe shape", "Fringe contrast", "Fringe width", "Fringe color"],
        answer: "Fringe width"
      },
      {
        question: "The wedge angle is typically...",
        options: ["Very large", "Exactly 90°", "Very small", "Unstable"],
        answer: "Very small"
      }
    ]
  },
  "4": {
    experiment: "Zener Diode",
    questions: [
      {
        question: "Zener diode is mainly used for...",
        options: ["Amplification", "Rectification", "Voltage regulation", "Switching"],
        answer: "Voltage regulation"
      },
      {
        question: "In Zener breakdown, the current...",
        options: ["Increases linearly", "Decreases", "Stays constant", "Increases rapidly"],
        answer: "Increases rapidly"
      },
      {
        question: "Zener diode operates in which region?",
        options: ["Forward bias", "Reverse bias", "Cut-off", "Saturation"],
        answer: "Reverse bias"
      },
      {
        question: "What is the symbol of Zener diode?",
        options: ["Same as PN diode", "Arrow + bar", "Bar with zigzag", "Circle with arrow"],
        answer: "Arrow + bar"
      },
      {
        question: "Zener voltage is...",
        options: ["Forward voltage", "Breakdown voltage", "Peak voltage", "Critical voltage"],
        answer: "Breakdown voltage"
      }
    ]
  },
  "5": {
    experiment: "Newton's Ring",
    questions: [
      {
        question: "Newton's Rings are formed due to...",
        options: ["Refraction", "Polarization", "Interference", "Diffraction"],
        answer: "Interference"
      },
      {
        question: "Rings are observed in...",
        options: ["Reflection", "Transmission", "Absorption", "Emission"],
        answer: "Reflection"
      },
      {
        question: "The rings are...",
        options: ["Rectangular", "Concentric circles", "Parallel lines", "Spirals"],
        answer: "Concentric circles"
      },
      {
        question: "The radius of rings depends on...",
        options: ["Wavelength and lens curvature", "Only on wavelength", "Only on thickness", "Material only"],
        answer: "Wavelength and lens curvature"
      },
      {
        question: "Dark rings occur due to...",
        options: ["Constructive interference", "Destructive interference", "Scattering", "Absorption"],
        answer: "Destructive interference"
      }
    ]
  },
  "6": {
    experiment: "Hall Effect",
    questions: [
      {
        question: "Hall effect is observed when a current-carrying conductor is placed in...",
        options: ["Electric field", "Magnetic field", "Light beam", "Vacuum"],
        answer: "Magnetic field"
      },
      {
        question: "Hall voltage is proportional to...",
        options: ["Magnetic field", "Current", "Thickness", "Both A and B"],
        answer: "Both A and B"
      },
      {
        question: "Which charge carriers cause Hall Effect in metals?",
        options: ["Electrons", "Holes", "Protons", "Neutrons"],
        answer: "Electrons"
      },
      {
        question: "Hall Effect is used to measure...",
        options: ["Current", "Voltage", "Magnetic field", "Frequency"],
        answer: "Magnetic field"
      },
      {
        question: "Unit of Hall coefficient is...",
        options: ["m³/C", "C/m³", "T·m/A", "A/m²"],
        answer: "m³/C"
      }
    ]
  },
  "7": {
    experiment: "Planck's Constant",
    questions: [
      {
        question: "Planck's constant relates energy to...",
        options: ["Mass", "Wavelength", "Frequency", "Force"],
        answer: "Frequency"
      },
      {
        question: "Unit of Planck's constant is...",
        options: ["Joule", "Joule-second", "Watt", "Electron-volt"],
        answer: "Joule-second"
      },
      {
        question: "Photoelectric effect is used to determine...",
        options: ["Resistance", "Planck's constant", "Charge", "Magnetic field"],
        answer: "Planck's constant"
      },
      {
        question: "Einstein explained photoelectric effect using...",
        options: ["Newton's laws", "Quantum theory", "Wave theory", "String theory"],
        answer: "Quantum theory"
      },
      {
        question: "Value of Planck's constant (approx.) is...",
        options: ["6.63 × 10⁻³⁴ Js", "3.00 × 10⁸ Js", "1.6 × 10⁻¹⁹ Js", "9.1 × 10⁻³¹ Js"],
        answer: "6.63 × 10⁻³⁴ Js"
      }
    ]
  },
  "8": {
    experiment: "He-Ne Laser",
    questions: [
      {
        question: "The active medium of a He-Ne laser is...",
        options: ["Solid", "Liquid", "Gas", "Plasma"],
        answer: "Gas"
      },
      {
        question: "Which gases are used in He-Ne laser?",
        options: ["Helium and Nitrogen", "Helium and Neon", "Hydrogen and Neon", "Oxygen and Helium"],
        answer: "Helium and Neon"
      },
      {
        question: "What is the typical wavelength of He-Ne laser?",
        options: ["532 nm", "1550 nm", "633 nm", "405 nm"],
        answer: "633 nm"
      },
      {
        question: "He-Ne laser produces...",
        options: ["White light", "Incoherent light", "Monochromatic light", "Broad spectrum"],
        answer: "Monochromatic light"
      },
      {
        question: "Main use of He-Ne laser is in...",
        options: ["CD reading", "Laser pointers", "Scientific research", "Laser printing"],
        answer: "Scientific research"
      }
    ]
  }
};

// Post-test questions
export const postTestQuestions: Record<string, ExperimentQuestions> = {
  "1": {
    experiment: "IV-Characteristics",
    questions: [
      {
        question: "What does the slope of the IV graph represent?",
        options: ["Resistance", "Current", "Voltage", "Power"],
        answer: "Resistance"
      },
      {
        question: "Which component shows a linear IV relationship?",
        options: ["Resistor", "Diode", "LED", "Capacitor"],
        answer: "Resistor"
      },
      {
        question: "Which of the following does not follow Ohm's Law?",
        options: ["Resistor", "Ideal conductor", "Diode", "Wire"],
        answer: "Diode"
      },
      {
        question: "When voltage is doubled across a resistor, the current will...",
        options: ["Double", "Halve", "Stay the same", "Be zero"],
        answer: "Double"
      },
      {
        question: "IV characteristics help determine a component's...",
        options: ["Resistance", "Power", "Shape", "Temperature"],
        answer: "Resistance"
      }
    ]
  },
  "2": {
    experiment: "Numerical Aperture",
    questions: [
      {
        question: "Higher numerical aperture means...",
        options: ["Less light acceptance", "More light acceptance", "Narrow beam", "Less bandwidth"],
        answer: "More light acceptance"
      },
      {
        question: "Numerical aperture is related to...",
        options: ["Acceptance angle", "Refraction index", "Fiber length", "All of the above"],
        answer: "All of the above"
      },
      {
        question: "The unit of numerical aperture is...",
        options: ["Meters", "Radians", "Dimensionless", "dB"],
        answer: "Dimensionless"
      },
      {
        question: "If core refractive index increases, NA...",
        options: ["Increases", "Decreases", "Unchanged", "Becomes zero"],
        answer: "Increases"
      },
      {
        question: "NA affects which of the following the most?",
        options: ["Signal power", "Data rate", "Light gathering", "Temperature"],
        answer: "Light gathering"
      }
    ]
  },
  "3": {
    experiment: "Wedge Shape",
    questions: [
      {
        question: "Wedge-shaped films show interference due to...",
        options: ["Variable film thickness", "Refraction", "Absorption", "Transmission"],
        answer: "Variable film thickness"
      },
      {
        question: "Fringe spacing depends on...",
        options: ["Wedge angle", "Light intensity", "Material", "Refractive index"],
        answer: "Wedge angle"
      },
      {
        question: "What is used to form the wedge shape?",
        options: ["Two plates and a spacer", "Prism", "Lens", "Glass tube"],
        answer: "Two plates and a spacer"
      },
      {
        question: "Interference occurs due to...",
        options: ["Constructive and destructive interference", "Absorption", "Reflection only", "Diffraction"],
        answer: "Constructive and destructive interference"
      },
      {
        question: "Why is monochromatic light used?",
        options: ["To avoid scattering", "To see colored patterns", "To get distinct fringes", "For absorption"],
        answer: "To get distinct fringes"
      }
    ]
  },
  "4": {
    experiment: "Zener Diode",
    questions: [
      {
        question: "In reverse breakdown, Zener diode...",
        options: ["Conducts", "Blocks current", "Acts as open circuit", "Heats up"],
        answer: "Conducts"
      },
      {
        question: "The breakdown voltage is also called...",
        options: ["Zener voltage", "Forward voltage", "Bias voltage", "Critical voltage"],
        answer: "Zener voltage"
      },
      {
        question: "What remains constant in the breakdown region?",
        options: ["Voltage", "Current", "Resistance", "Power"],
        answer: "Voltage"
      },
      {
        question: "Zener diode symbol is like diode but with...",
        options: ["Bent line", "Zigzag", "Circle", "Arrow"],
        answer: "Bent line"
      },
      {
        question: "Zener diode is mainly used in...",
        options: ["Regulators", "Amplifiers", "Oscillators", "Multipliers"],
        answer: "Regulators"
      }
    ]
  },
  "5": {
    experiment: "Newton's Ring",
    questions: [
      {
        question: "Dark rings in Newton's Ring are due to...",
        options: ["Destructive interference", "Constructive interference", "Refraction", "Scattering"],
        answer: "Destructive interference"
      },
      {
        question: "The shape of the ring depends on...",
        options: ["Curvature of lens", "Light intensity", "Material only", "Temperature"],
        answer: "Curvature of lens"
      },
      {
        question: "Radius of ring increases with...",
        options: ["Ring number", "Light power", "Pressure", "Refractive index"],
        answer: "Ring number"
      },
      {
        question: "To observe Newton's Ring, we use...",
        options: ["Monochromatic light", "White light", "LED", "Sunlight"],
        answer: "Monochromatic light"
      },
      {
        question: "Rings are observed through...",
        options: ["Reflection", "Absorption", "Refraction", "Scattering"],
        answer: "Reflection"
      }
    ]
  },
  "6": {
    experiment: "Hall Effect",
    questions: [
      {
        question: "Hall Effect voltage appears across...",
        options: ["Width of the sample", "Length of the sample", "Thickness of sample", "All directions"],
        answer: "Width of the sample"
      },
      {
        question: "Hall Effect is used to find...",
        options: ["Charge type", "Resistance", "Length", "Voltage drop"],
        answer: "Charge type"
      },
      {
        question: "In semiconductors, Hall effect is due to...",
        options: ["Electrons and holes", "Protons", "Neutrons", "Magnetic waves"],
        answer: "Electrons and holes"
      },
      {
        question: "Hall Effect is maximum when...",
        options: ["Current is perpendicular to magnetic field", "Current and field are parallel", "No current", "No field"],
        answer: "Current is perpendicular to magnetic field"
      },
      {
        question: "Direction of Hall current is given by...",
        options: ["Fleming's left-hand rule", "Right-hand rule", "Ohm's law", "Lenz's law"],
        answer: "Right-hand rule"
      }
    ]
  },
  "7": {
    experiment: "Planck's Constant",
    questions: [
      {
        question: "Planck's constant is determined from...",
        options: ["Photoelectric experiment", "KVL", "Ampere's law", "Snell's law"],
        answer: "Photoelectric experiment"
      },
      {
        question: "Photoelectric effect proves...",
        options: ["Particle nature of light", "Wave nature", "Mass conservation", "Field theory"],
        answer: "Particle nature of light"
      },
      {
        question: "In photoelectric effect, energy of photon is...",
        options: ["E = hν", "E = mc²", "E = IR", "E = qV"],
        answer: "E = hν"
      },
      {
        question: "Planck's constant is used in...",
        options: ["Quantum theory", "Classical mechanics", "Thermodynamics", "Kinetics"],
        answer: "Quantum theory"
      },
      {
        question: "SI unit of Planck's constant is...",
        options: ["J·s", "W", "V", "Coulomb"],
        answer: "J·s"
      }
    ]
  },
  "8": {
    experiment: "He-Ne Laser",
    questions: [
      {
        question: "He-Ne laser emits light at which wavelength?",
        options: ["633 nm", "532 nm", "450 nm", "1064 nm"],
        answer: "633 nm"
      },
      {
        question: "Which gas mixture is used in He-Ne laser?",
        options: ["Helium and Neon", "Hydrogen and Helium", "Neon and Argon", "Neon and Nitrogen"],
        answer: "Helium and Neon"
      },
      {
        question: "He-Ne laser is used in...",
        options: ["Holography", "Cooking", "Cooling", "Data storage"],
        answer: "Holography"
      },
      {
        question: "The light from a He-Ne laser is...",
        options: ["Monochromatic and coherent", "White and coherent", "Broad spectrum", "Polychromatic"],
        answer: "Monochromatic and coherent"
      },
      {
        question: "Active medium of He-Ne laser is in...",
        options: ["Gas phase", "Solid phase", "Liquid phase", "Plasma"],
        answer: "Gas phase"
      }
    ]
  }
};

// Helper function to get questions based on type and id
export function getQuestions(type: "pre-test" | "post-test", id: string): ExperimentQuestions | null {
  const questions = type === "pre-test" ? preTestQuestions : postTestQuestions;
  return questions[id] || null;
}