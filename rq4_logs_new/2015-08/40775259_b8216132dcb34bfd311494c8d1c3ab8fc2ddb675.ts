/**
 * Created by vmlf on 15-08-2015.
 */

declare var $;

class ErrorController {
    static resetErrorDisplay(): void {
        $("#errorDisplay").hide();
    }

    static displayError(err: Error): void {
        $("#errorMessage").html(err);
        $("#errorDisplay").show();
    }
}

export = ErrorController;