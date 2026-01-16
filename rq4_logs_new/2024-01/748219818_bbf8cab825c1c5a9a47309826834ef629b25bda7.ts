import { defineNuxtPlugin } from "#app";
import { useRuntimeConfig } from "#imports";

// Function to create and append a script tag for Inkeep
function createInkeepScriptElement() {
  const WIDGET_VERSION = "@0.2.258";
  const script = document.createElement("script");
  script.src = `https://unpkg.com/@inkeep/widgets-embed${WIDGET_VERSION}/dist/embed.js`;
  script.type = "module";
  script.id = "inkeep-script";
  script.defer = true;
  document.head.appendChild(script);
  return script;
}

// Function to create and append a div tag for Inkeep
function createInkeepDivElement() {
  const div = document.createElement("div");
  div.id = "inkeep";
  return div;
}

// Function to initialize the Inkeep widget with configuration and theme mode
function initializeInkeepWidget(inkeepConfig: any, isCurrentlyDark: boolean) {
  const isChatButton = inkeepConfig?.componentType === "ChatButton";
  const inkeepScript = createInkeepScriptElement();

  inkeepScript.addEventListener("load", () => {
    const inkeepWidget = Inkeep().embed({
      componentType: inkeepConfig?.componentType,
      targetElement: isChatButton ? null : document.getElementById("inkeep"), // Use the created div
      properties: {
        ...inkeepConfig,
        baseSettings: {
          ...inkeepConfig.baseSettings,
          colorMode: {
            forcedColorMode: isCurrentlyDark ? "dark" : "light",
          },
        },
      },
    });

    const observer = new MutationObserver((mutationsList) => {
      for (const mutation of mutationsList) {
        if (mutation.attributeName === "class") {
          const isDark = mutation.target.classList.contains("dark");
          inkeepWidget.render({
            baseSettings: {
              ...inkeepConfig.baseSettings,
              colorMode: {
                forcedColorMode: isDark ? "dark" : "light",
              },
            },
          });
        }
      }
    });

    observer.observe(document.documentElement, { attributes: true });
  });
}

// Define Nuxt.js plugin
export default defineNuxtPlugin({
  name: "inkeep-widget",
  async setup() {
    const {
      public: { inkeepConfig },
    } = useRuntimeConfig();

    const isChatButton = inkeepConfig?.componentType === "ChatButton";

    if (!inkeepConfig) {
      throw new Error(
        "Configuration Error: inkeepConfig is missing in the Nuxt configuration file"
      );
    }

    const initializeSearchBar = () => {
      const inkeepDiv = createInkeepDivElement();
      const rightSectionBar = document.querySelector(".right");
      if (rightSectionBar) {
        const observer = new MutationObserver(() => {
          rightSectionBar.appendChild(inkeepDiv);
        });
        observer.observe(document.documentElement, { attributes: true });
      }
    };

    if (document) {
      const isCurrentlyDark =
        document.documentElement.classList.contains("dark");

      if (!isChatButton) {
        initializeSearchBar();
      }

      // Initialize the Inkeep widget
      initializeInkeepWidget(inkeepConfig, isCurrentlyDark);
    }
  },
});