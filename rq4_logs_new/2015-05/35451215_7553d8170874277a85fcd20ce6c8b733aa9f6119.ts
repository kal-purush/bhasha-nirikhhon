/**
 * Created by riana on 15/05/15.
 */


/// <reference path='../typings/_all.ts' />
/// <reference path="Item.ts"/>

module ngFragmentsDemo {

    'use strict';

    export interface DNDRawScope extends ng.IScope {
        ctrl : DnDRawController;
    }

    export class DnDRawController {

        public static $inject = [
            '$scope'
        ];

        public itemPrototypes:Item[] = [];

        sequence:Item = new Item();

        dragController:any;

        constructor(private $scope:DNDRawScope) {
            $scope.ctrl = this;

            this.itemPrototypes = [].concat(ngFragmentsDemo.ItemPrototypes);
            this.sequence.kind = 'action';
            this.sequence.containsChildren = true;
            this.sequence.label = 'Sequence E';


            var self = this;
            this.dragController = {

                pull: false,

                accept: function(newItem, index){
                    return false;
                },

                itemAt: function(index){
                    return self.itemPrototypes[index];
                },

                add: function(newItem, index){


                },

                remove: function(item){

                }
            }

        }

    }

}