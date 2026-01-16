const assert = require('assert');
const FlyJsonQL = require('../src/flyjsonql');

describe('CRUD test', function() {

    var data1 = [
        {user_id:1,name:'budi',age:10},
        {user_id:5,name:'wawan',age:20},
        {user_id:3,name:'tono',age:30}
    ];

    it('insert data', function(done) {
        const jsonql = new FlyJsonQL();
        var q = [
            {
                insert:{
                    into:data1,
                    values:[
                        {user_id:7,name:'muhammad',age:17},
                        {user_id:8,name:'aziz',age:18},
                        {user_id:9,name:'alfian',age:19}
                    ]
                }
            }
        ];

        jsonql.query(q).exec(function(error,data) {
            assert.equal(data[0].response.count,3);
            done();
        });
    });

    it('update data', function(done) {
        const jsonql = new FlyJsonQL();
        var q = [
            {
                update:{
                    from:data1,
                    where:[
                        ['user_id','==','1']
                    ],
                    key:'user_id',
                    set:{
                        user_id:1,
                        name:'budi setiawan',
                        age:23
                    }
                }
            }
        ];

        jsonql.query(q).exec(function(error,data) {
            assert.equal(data[0].response.count,1);
            done();
        });
    });

    it('modify data', function(done) {
        const jsonql = new FlyJsonQL();
        var q = [
            {
                modify:{
                    from:data1,
                    where:[
                        ['user_id','==','1']
                    ],
                    key:'user_id',
                    set:{
                        name:'budi setiawan',
                        age:23
                    }
                }
            }
        ];

        jsonql.query(q).exec(function(error,data) {
            assert.equal(data[0].response.count,1);
            done();
        });
    });

    it('delete data', function(done) {
        const jsonql = new FlyJsonQL();
        var q = [
            {
                delete:{
                    from:data1,
                    or:[
                        {
                            where:[
                                ['user_id','==','1']
                            ]
                        },
                        {
                            where:[
                                ['user_id','==','5']
                            ]
                        }
                    ],
                    key:'user_id'
                }
            }
        ];

        jsonql.query(q).exec(function(error,data) {
            assert.equal(data[0].response.count,2);
            done();
        });
    });

});