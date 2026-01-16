class DynamicImageGenerator {
  constructor() {
    this.imageCache = {};
    this.initStyles();
  }

  initStyles() {
    const style = document.createElement("style");
    style.textContent = `
        .generated-image-container {
          position: relative;
          width: 100%;
          max-width: 1024px;
          margin: 20px auto;
          border-radius: 16px;
          overflow: hidden;
          box-shadow: 0 10px 30px rgba(0,0,0,0.2);
          aspect-ratio: 1/1;
          background: linear-gradient(45deg, #f3f4f6, #e5e7eb);
          animation: fadeIn 0.8s ease-out;
        }
        
        .generated-image {
          width: 100%;
          height: 100%;
          object-fit: cover;
          transition: transform 0.5s ease, opacity 0.8s ease;
        }
        
        .generated-image.loading {
          opacity: 0;
          transform: scale(0.95);
        }
        
        .generated-image.loaded {
          opacity: 1;
          transform: scale(1);
        }
        
        .image-controls {
          display: flex;
          gap: 12px;
          justify-content: center;
          margin: 20px 0;
          flex-wrap: wrap;
        }
        
        .generate-btn {
          background: linear-gradient(135deg, #6366f1, #8b5cf6);
          color: white;
          border: none;
          padding: 12px 24px;
          border-radius: 50px;
          cursor: pointer;
          font-weight: 600;
          box-shadow: 0 4px 6px rgba(0,0,0,0.1);
          transition: all 0.3s ease;
        }
        
        .generate-btn:hover {
          transform: translateY(-2px);
          box-shadow: 0 6px 12px rgba(0,0,0,0.15);
        }
        
        .generate-btn:active {
          transform: translateY(0);
        }
        
        .image-prompt-input {
          flex: 1;
          min-width: 300px;
          max-width: 500px;
          padding: 12px 16px;
          border-radius: 50px;
          border: 2px solid #e5e7eb;
          font-size: 16px;
          transition: all 0.3s ease;
        }
        
        .image-prompt-input:focus {
          border-color: #6366f1;
          outline: none;
          box-shadow: 0 0 0 3px rgba(99,102,241,0.2);
        }
        
        @keyframes fadeIn {
          from { opacity: 0; transform: translateY(10px); }
          to { opacity: 1; transform: translateY(0); }
        }
        
        @keyframes pulse {
          0%, 100% { opacity: 0.6; }
          50% { opacity: 1; }
        }
        
        .loading-indicator {
          position: absolute;
          top: 50%;
          left: 50%;
          transform: translate(-50%, -50%);
          color: white;
          font-size: 18px;
          font-weight: bold;
          text-shadow: 0 2px 4px rgba(0,0,0,0.3);
          animation: pulse 1.5s infinite;
        }
      `;
    document.head.appendChild(style);
  }

  async generateImage(prompt, width = 1024, height = 1024, model = "flux") {
    const cacheKey = `${prompt}-${width}-${height}-${model}`;

    if (this.imageCache[cacheKey]) {
      return this.imageCache[cacheKey];
    }

    const container = document.createElement("div");
    container.className = "generated-image-container";

    const img = document.createElement("img");
    img.className = "generated-image loading";
    img.alt = prompt;
    img.crossOrigin = "anonymous"; // Add this for CORS handling

    const loadingText = document.createElement("div");
    loadingText.className = "loading-indicator";
    loadingText.textContent = "Generating artwork...";

    container.appendChild(img);
    container.appendChild(loadingText);

    try {
      const url = `https://image.pollinations.ai/prompt/${encodeURIComponent(
        prompt
      )}?width=${width}&height=${height}&seed=${Math.floor(
        Math.random() * 1000
      )}&model=${model}`;

      // Add error handler for the image
      img.onerror = () => {
        throw new Error("Failed to load image");
      };

      await new Promise((resolve, reject) => {
        img.onload = () => {
          img.classList.remove("loading");
          img.classList.add("loaded");
          container.removeChild(loadingText);
          resolve();
        };
        img.src = url;
      });

      this.imageCache[cacheKey] = { url, element: container };
      return this.imageCache[cacheKey];
    } catch (error) {
      console.error("Error generating image:", error);
      container.remove();
      throw error;
    }
  }

  createImageControls(onGenerate) {
    const controlsContainer = document.createElement("div");
    controlsContainer.className = "image-controls";

    const promptInput = document.createElement("input");
    promptInput.className = "image-prompt-input";
    promptInput.placeholder = "Describe the image you want to generate...";
    promptInput.value = "A beautiful landscape";

    const generateBtn = document.createElement("button");
    generateBtn.className = "generate-btn";
    generateBtn.textContent = "Generate Image";

    generateBtn.addEventListener("click", () => {
      const prompt = promptInput.value.trim();
      if (prompt) {
        onGenerate(prompt);
      }
    });

    controlsContainer.appendChild(promptInput);
    controlsContainer.appendChild(generateBtn);

    return controlsContainer;
  }
}