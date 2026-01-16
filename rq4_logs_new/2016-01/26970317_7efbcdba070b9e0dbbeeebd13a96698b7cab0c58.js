var expect = chai.expect;
var Dialog = require('gelato/dialog');

describe('GelatoDialog', function() {
    var dialog;

    before(function() {
        Backbone.$('gelato-application').append('<test-view></test-view>');
    });

    beforeEach(function() {
        Backbone.$('test-view').html('<div id="dialog-container"></div>');
        dialog = new Dialog();
        dialog.template = '<gelato-dialog data-name="test">';
        dialog.template += '<div class="modal fade" role="dialog">';
        dialog.template += '<div class="modal-dialog"></div>';
        dialog.template += '</div></gelato-dialog>';
    });

    it('close()', function(done) {
        dialog.open();
        dialog.close();
        dialog.on('hidden', function() {
            expect(Backbone.$('#dialog-container').find('gelato-dialog')).to.have.length(1);
            done();
        });
    });

    it('open()', function(done) {
        dialog.open();
        dialog.on('shown', function() {
            expect(Backbone.$('#dialog-container').find('gelato-dialog')).to.have.length(1);
            dialog.close();
            done();
        });
    });

    afterEach(function() {
        Backbone.$('.modal-backdrop').remove();
        Backbone.$('body').removeClass('modal-open');
    });

    after(function() {
        Backbone.$('test-view').remove();
    });

});