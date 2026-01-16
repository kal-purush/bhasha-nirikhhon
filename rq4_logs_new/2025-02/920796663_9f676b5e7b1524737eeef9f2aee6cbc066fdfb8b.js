import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
  static targets = ["icon", "label"]
  static values = {
    text: String
  }

  connect() {
    this.originalText = this.labelTarget.textContent
    
    // Listen for Turbo form submission events
    document.addEventListener("turbo:submit-start", this.handleSubmitStart.bind(this))
    document.addEventListener("turbo:submit-end", this.handleSubmitEnd.bind(this))
  }

  disconnect() {
    // Clean up event listeners
    document.removeEventListener("turbo:submit-start", this.handleSubmitStart)
    document.removeEventListener("turbo:submit-end", this.handleSubmitEnd)
  }

  handleSubmitStart(event) {
    if (event.target.contains(this.element)) {
      this.start()
    }
  }

  handleSubmitEnd(event) {
    if (event.target.contains(this.element)) {
      this.stop()
    }
  }

  start() {
    this.element.disabled = true
    this.labelTarget.textContent = this.textValue
    this.iconTarget.classList.add("animate-spin")
    this.iconTarget.style.animationDuration = "1s"
  }

  stop() {
    this.element.disabled = false
    this.labelTarget.textContent = this.originalText
    this.iconTarget.classList.remove("animate-spin")
    this.iconTarget.style.animationDuration = ""
  }
} 