'use strict';

import Vue from 'vue';

export default class TeacherService {
    constructor(){
        this.resourceAll = Vue.resource('/api/teachers');
    }
    
    setTeacher(id){
        return this.resourceAll.save({id})
    }
    unsetTeacher(id){
        return this.resourceAll.delete({id})
    }

}