function addCardAndModal(id, title, imgSrc, contentCard,contentModal, link) {
    $(document).ready(function() {
        if (link == undefined) {
            var cardAndModalHTML = `
            <div class="card" id="${id}" style="width: 18rem;">
                <img src="${imgSrc}" class="card-img-top" alt="...">
                <div class="card-body">
                    <h5 class="card-title">${title}</h5>
                    <p class="card-text">${contentCard}</p>
                    <button type="button" class="btn btn-primary" data-bs-toggle="modal" data-bs-target="#${id}Modal">
                        Saiba mais
                    </button>
                </div>
            </div>

            <div class="modal fade" id="${id}Modal" tabindex="-1" aria-labelledby="${id}ModalLabel" aria-hidden="true">
                <div class="modal-dialog">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h1 class="modal-title fs-5" id="${id}ModalLabel">${title}</h1>
                            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                        </div>
                        <div class="modal-body">
                            <img src="${imgSrc}" class="card-img-top" alt="...">
                            ${contentModal}
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn-close- btn btn-secondary" data-bs-dismiss="modal">Close</button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        }
        else{
            var cardAndModalHTML = `
            <div class="card" id="${id}" style="width: 18rem;">
                <img src="${imgSrc}" class="card-img-top" alt="...">
                <div class="card-body">
                    <h5 class="card-title">${title}</h5>
                    <p class="card-text">${contentCard}</p>
                    <button type="button" class="btn btn-primary" data-bs-toggle="modal" data-bs-target="#${id}Modal">
                    Saiba mais + Link
                    </button>
                </div>
            </div>

            <div class="modal fade" id="${id}Modal" tabindex="-1" aria-labelledby="${id}ModalLabel" aria-hidden="true">
                <div class="modal-dialog">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h1 class="modal-title fs-5" id="${id}ModalLabel">${title}</h1>
                            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                        </div>
                        <div class="modal-body">
                            <img src="${imgSrc}" class="card-img-top" alt="...">
                            ${contentModal}

                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn-close- btn btn-secondary" data-bs-dismiss="modal">Close</button>
                            <button onclick="window.location.href='${link}'" type="button" class="btn btn-primary link-colorido">Inscrições Aqui!</button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        }
        

        $(".add-element").append(cardAndModalHTML);
    });
}

$(document).ready(function() {
    $.getJSON("cards.json", function(cards) {
        for (var i = 0; i < cards.length; i++) {
            var card = cards[i];
            addCardAndModal(card.id, card.title, card.imgSrc, card.contentCard, card.contentModal, card.elementSelector, card.link);
        }
      
    });
});