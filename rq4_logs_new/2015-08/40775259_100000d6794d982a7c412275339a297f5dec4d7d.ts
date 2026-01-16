/**
 * Created by vmlf on 13-08-2015.
 */

import PainterDetailsController = require('./PainterDetailsController');
import PaintersService = require('../services/PaintersDataService');
import Painter = require('../models/Painter');

declare var $;

class PaintersListController {

    private painterDetails: PainterDetailsController;
    private paintersService: PaintersService;

    constructor() {
        this.painterDetails = new PainterDetailsController();
        this.paintersService = new PaintersService();
    }

    showPainters(): void {

        $("#errorDisplay").hide();
        this.paintersService.getPainters((err, painters) => {
            if (err) {
                $("#errorMessage").html(err);
                $("#errorDisplay").show();
            }
            else {
                this.renderPainters($("#paintersList"), painters);
            }
        });

    }

    private renderPainters(container:HTMLElement, paintersList: Painter[]): void {
        // clear previous container contents
        $(container).empty();

        // append each new painter
        if (paintersList) {
            paintersList.forEach((p) => {
                var html = `
                    <div class="painter col-sm-6 col-md-3 col-lg-2">
                        <img class="img-responsive" src="` + p.image + `" />
                        <h3>` + p.name + `</h3>
                    </div>
                `;
                var painterDiv = $(html);
                painterDiv.click(() => this.painterDetails.showDetails(p));

                $(container).append(painterDiv);
            });
        }
    }
}

export = PaintersListController;