class Button
{
    public Image: string;
    public Text: string;
    public Class: string;

    public Data: { [key: string]: any };

    public Command: (e: JQueryEventObject) => any;

    To$(): JQuery
    {
        var button = $("<button>")
            .attr("type", "button")
            .addClass("btn " + this.Class);

        if (this.Image)
        {
            var img = $("<span>")
                .addClass("glyphicon glyphicon-" + this.Image)
                .attr("aria-hidden", "true");

            button.append(img);
        }

        var result = $("<span>").addClass("result").html(this.Text);
        button.append(result);

        button.click(this.Command);

        if (this.Data)
        {
            button.data(this.Data);
        }

        return button;
    }
}