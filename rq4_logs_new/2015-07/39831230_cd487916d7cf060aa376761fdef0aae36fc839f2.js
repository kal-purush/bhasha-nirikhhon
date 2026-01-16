/**
 * Created by vitali.nalivaika on 30.07.2015.
 */


define(["jquery", "knockout-3.3.0"],function($, ko) {
        function MessagesViewModel() {
            'use strict';

            var self = this;

            self.idRoom = ko.observable(0);
            self.idUser = ko.observable('');
            self.message = ko.observable('');
            self.external = ko.observable('');

            self.UserConstructor = function () {
                this.idRoom = 0;
                this.idUser = '';
                this.message = '';
                this.external = '';
                return this;
            };

        }
        return new MessagesViewModel();
    }
);


/*var promiseCount = 0;
function testPromise() {
    var thisPromiseCount = ++promiseCount;

    var log = document.getElementById('log');
    log.insertAdjacentHTML('beforeend', thisPromiseCount +
        ') ������ (������ ����������� ����)');

    // ������ ��������, ������������ 'result' (�� ��������� 3-� ������)
    var p1 = new Promise(
        // ������� ���������� ��������� ��������� ������� ���
        // ��������� ��������
        function(resolve, reject) {
            log.insertAdjacentHTML('beforeend', thisPromiseCount +
                ') ������ �������� (������ ������������ ����)');
            // ��� ����� ���� ������ �������������
            window.setTimeout(
                function() {
                    // �������� ���������!
                    resolve(thisPromiseCount)
                }, Math.random() * 2000 + 1000);
        });

    // ���������, ��� ������� � ����������� ���������
    p1.then(
        // ���������� � ��������
        function(val) {
            log.insertAdjacentHTML('beforeend', val +
                ') �������� ��������� (����������� ��� ��������)');
        });

    log.insertAdjacentHTML('beforeend', thisPromiseCount +
        ') �������� ������� (���������� ��� ��������)');
}*/