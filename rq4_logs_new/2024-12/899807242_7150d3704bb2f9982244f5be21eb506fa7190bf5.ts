export const ticketMetadata = {
  status: {
    Open: {
      text: "Abierto",
      icon: "CircleDot",
      color: "#3498db", // Azul
    },
    Closed: {
      text: "Cerrado",
      icon: "CheckCircle2",
      color: "#2ecc71", // Verde
    },
    InProgress: {
      text: "En progreso",
      icon: "Timer",
      color: "#f1c40f", // Amarillo
    },
  },
  type: {
    Bug: {
      text: "Bug",
      icon: "Bug",
      color: "#e74c3c", // Rojo
    },
    Feature: {
      text: "Característica",
      icon: "Lightbulb",
      color: "#9b59b6", // Morado
    },
    Question: {
      text: "Pregunta",
      icon: "HelpCircle",
      color: "#3498db", // Azul
    },
  },
  priority: {
    Low: {
      text: "Baja",
      icon: "ArrowDown",
      color: "#2ecc71", // Verde
    },
    Medium: {
      text: "Media",
      icon: "ArrowRight",
      color: "#f1c40f", // Amarillo
    },
    High: {
      text: "Alta",
      icon: "ArrowUp",
      color: "#e74c3c", // Rojo
    },
  },
};