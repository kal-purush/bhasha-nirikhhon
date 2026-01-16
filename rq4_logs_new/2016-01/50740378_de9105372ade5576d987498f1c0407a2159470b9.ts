import * as pgLib from '../pgLib';

export var f1 = async  (cliente,params) =>
{
    var r = [];
    // diferenets accesos kkka las tablas
    await pgLib.q(cliente,"insert into p_persona values($1,$2,$3,$4,$5,$6)",[Math.random(),'a','b','c','d','e']);
    r.push( await pgLib.q(cliente,"select count(*) contador1  from p_persona;select count(*) contador from p_persona",[]));
    r.push( await pgLib.q(cliente,"select count(*) c1 from p_persona;select count(*) contador2 from p_persona",[]));
    return r;
}

export var f2 = async  (cliente,params) =>
{
    var r = [];
    // diferenets accesos a las tablas
    await pgLib.q(cliente,"insert into p_persona values($1,$2,$3,$4,$5,$6)",[Math.random(),'a','b','c','d','e']);
    r.push( await pgLib.q(cliente,"select count(*) contador22221yyy  from p_persona;select count(*) contador from p_persona",[]));
    r.push( await pgLib.q(cliente,"select count(*) c1 from p_persona;select count(*) contador2 from p_persona",[]));
    return r;
}


export var f3 = async  (cliente,params) =>
{
    var r = [];
    // diferenets accesos a las tablas
    await pgLib.q(cliente,"insert into p_persona values($1,$2,$3,$4,$5,$6)",[Math.random(),'a','b','c','d','e']);
    r.push( await pgLib.q(cliente,"select count(*) contador333333  from p_persona;select count(*) contador from p_persona",[]));
    r.push( await pgLib.q(cliente,"select count(*) c1 from p_persona;select count(*) contador2 from p_persona",[]));
    return r;
}