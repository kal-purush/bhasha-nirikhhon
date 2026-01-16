//S11

//Q1

function scale_stream(c, stream) {
    return stream_map(x => c * x, stream);
}
const A = pair(1, () => scale_stream(2, A));

//a stream of powers of two starting from 2^0, 2^1, 2^2 
//eval_stream(A, 10);

function mul_streams(a,b) {
    return pair(head(a) * head(b),
                () => mul_streams(stream_tail(a), stream_tail(b)));
}

const B = pair(1, () => mul_streams(B, integers_from(1)));

//a stream of numbers starting from 1 and the subsequent numbers are the product
//of the previous number and its position. 
//factorials 
//eval_stream(B, 10); //1, 1, 2, 6, 24, 120, 


//Q2

function add_streams(s1, s2) {
    return is_null(s1)
           ? s2
           : is_null(s2)
           ? s1
           : pair(head(s1) + head(s2), () => add_streams(stream_tail(s1), 
                                          stream_tail(s2)));
}

// function scale_stream(c, stream) {
//     return stream_map(x => c * x, stream);
// }

const add_series = add_streams;
const scale_series = scale_stream;

function negate_series(s) {
    return scale_series(-1, s);
}

function subtract_series(s1, s2) {
    return add_series(s1, negate_series(s2));
}

function coeffs_to_series(list_of_coeffs) {
    const zeros = pair(0, () => zeros);
    function iter(list) {
        return is_null(list)
               ? zeros 
               : pair(head(list), () => iter(tail(list)));
    }
    return iter(list_of_coeffs);
}

const coe = coeffs_to_series(list(1,3,4));

function fun_to_series(fun) {
    return stream_map(fun, integers_from(0));
}

const alt_ones1 = fun_to_series(x => math_pow(-1, x));
const alt_ones2 = pair(1, () => stream_map(x => - x, alt_ones2));
const alt_ones3 = pair(1, () => scale_series(-1, alt_ones3));
const alt_ones4 = pair(1, () => negate_series(alt_ones4));
const alt_ones5 = pair(1, () => pair(-1, () => alt_ones5));

//eval_stream(alt_ones5, 10);

const zeros1 = add_streams(alt_ones1,negate_series(alt_ones1));
const zeros2 = add_series(alt_ones1,negate_series(alt_ones1));
const zeros3 = scale_series(0, alt_ones1);
const zeros4 = fun_to_series(x => 0);
const zeros5 = subtract_series(alt_ones1, alt_ones1);
const zeros6 = add_series(alt_ones1, stream_tail(alt_ones1));

//eval_stream(zeros4, 10);

//S1. 1,1,1,1,1,1...
const S1 = pair(1, () => S1);
const S10 = fun_to_series(x => 1);
const S100 = pair(1, () => add_series(integers_from(2), negate_series(integers_from(1))));
const S1000 = fun_to_series(x => math_pow(1, x));

//eval_stream(S100, 10);

//S2. 1,2,3,4,5,6...
const S2 = fun_to_series(x => x + 1);
const S20 = pair(1, () => add_streams(S20, S1));
const S200 = integers_from(1);

eval_stream(S20, 10);


//In class Studio 

function stream_pairs(s) {
    return is_null(s)
            ? null
            : stream_append(
            stream_map(
            sn => pair(head(s), sn),
            stream_tail(s)),
            stream_pairs(stream_tail(s)));
}

const ints = enum_stream(1, 5); 
const ints1 = stream_pairs(ints);
//eval_stream(ints1, 10);
//a) creating a streams pairs 12,13,14,15,23,24,25,34,35,45
// all possible combinations of two from the stream. 

//b) creating a stream of pairs where the head of the stream is pair to each of 
// the remaining elements in the stream, the cycle repeats to the tail of the stream 
// till it encounter null and stops 

const integer = integers_from(1);
//const s22 = stream_pairs(integer);
//eval_stream(s22, 2);

//c) the infinite stream of pairs where 1 is paired with the remaining of the elements
// where 2 is paired with the remaining elements of integer and so on 
// infinite number of stream_append calls 



//d) 

function stream_append_pickle(xs, ys) {
    return is_null(xs)
            ? ys()
            : pair(head(xs),
            () => stream_append_pickle(stream_tail(xs),
            ys));
}
function stream_pairs2(s) {
    return is_null(s)
        ? null
        : stream_append_pickle(
            stream_map(
            sn => pair(head(s), sn),
            stream_tail(s)),
            () => stream_pairs2(stream_tail(s)));
}
//const s23 = stream_pairs2(integer);
//eval_stream(s23, 2);

// delayed operation of stream_append ()=> 

//e) pair of 12, 13, 14, 15, 16, ...

function stream_pairs3(s) {
    return stream_append(stream_map(x => pair(head(s), x), 
                stream_tail(s)), () => stream_pairs3(stream_tail(s)));
}

const s24 = stream_pairs3(integer);
eval_stream(s24, 10);