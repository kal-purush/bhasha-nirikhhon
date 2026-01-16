class UI {
	public hdlInputPercent1: HTMLElement;
	public hdlInputPercent2: HTMLElement;
	public hdlButtonRun: HTMLElement;
	public hdlOutput: HTMLElement;

	constructor() {
		this.hdlInputPercent1 = document.getElementById("InputPercent1");
		this.hdlInputPercent2 = document.getElementById("InputPercent2");
		this.hdlButtonRun = document.getElementById("ButtonRun");
		this.hdlOutput = document.getElementById("Output");

		this.hdlButtonRun.addEventListener("click", function() {
			Percent.Run();
		});

		console.log("UI ready...");
	}
}

new UI();