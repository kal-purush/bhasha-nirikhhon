var NewsletterForm = /** @class */ (function () {
    function NewsletterForm() {
        this.formElement = document.querySelector('form');
        this.subscribeButton = document.querySelector('#submit');
        this.newsletterContainer = document.querySelector('.newsletter');
        this.messageContainer = document.querySelector('.message');
        if (this.formElement && this.subscribeButton && this.newsletterContainer && this.messageContainer) {
            this.subscribeButton.addEventListener('click', this.handleSubscribe.bind(this));
        }
    }
    NewsletterForm.prototype.handleSubscribe = function (event) {
        event.preventDefault();
        // Hide the newsletter container and show the message container
        if (this.newsletterContainer && this.messageContainer) {
            this.newsletterContainer.style.display = 'none';
            this.messageContainer.style.display = 'block';
        }
        // Rest of your form submission logic can go here
    };
    return NewsletterForm;
}());
document.addEventListener('DOMContentLoaded', function () {
    new NewsletterForm();
});