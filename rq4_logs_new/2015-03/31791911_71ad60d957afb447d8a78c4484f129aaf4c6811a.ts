
// Lazy
class Lazy<a> {
	private val:a;
	constructor(private thunk:()=>a) {}

	public force():a {
		if(this.thunk) {
			this.val = this.thunk();
			delete this.thunk;
		}
		return this.val;
	}
}

function defer<a>(val:a):Lazy<a> {
	return new Lazy(() => val);
}

// Lazy functor
function l_fmap<a, b>(f:(val:a)=>b, l:Lazy<a>):Lazy<b> {
	return new Lazy(() => f(l.force()));
}

// LazyList
interface LazyList<a> {
	empty:boolean;
	destruct:<b>( f:(x:Lazy<a>, xs:Lazy<LazyList<a>>)=>b )=>b;
}

class LazyCons<a> implements LazyList<a> {
	public empty:boolean = false;

	constructor(public x:Lazy<a>, public xs:Lazy<LazyList<a>>) {}

	public destruct<b>( f:(x:Lazy<a>, xs:Lazy<LazyList<a>>)=>b ):b {
		return f(this.x, this.xs);
	}
}

var LazyNil:LazyList<any> = {
	empty: true,
	destruct: (f) => {
		throw new Error('Calling LazyNil.destruct');
	}
}

// LazyList functor
function ll_fmap<a, b>(f:(x:a)=>b, l:LazyList<a>):LazyList<b> {
	if(l.empty) return LazyNil;

	return l.destruct((lx, lxs) => {
		return new LazyCons(l_fmap(f, lx), l_fmap(xs => ll_fmap(f, xs), lxs));
	});
}

// LazyList utils
function repeat<a>(x:a):LazyList<a> {
	return new LazyCons<a>( defer(x), new Lazy(() => repeat(x)) );
}

function take<a>(n:number, l:LazyList<a>):LazyList<a> {
	if(!n) return LazyNil;
	if(l.empty) return LazyNil;

	return l.destruct((x, xs) => {
		return new LazyCons<a>(x, new Lazy(() => take(n-1, xs.force())));
	});
}

function skip<a>(n:number, l:LazyList<a>):LazyList<a> {
	if(l.empty) return LazyNil;
	if(!n) return l;

	return l.destruct((x, xs) => skip(n-1, xs.force()));
}

function tail<a>(l:LazyList<a>):LazyList<a> {
	return skip(1, l);
}

function iterate<a>(f:(v:a)=>a, v:a):LazyList<a> {
	return new LazyCons(
		defer(v),
		new Lazy(() => iterate(f, f(v)))
	);
}

function toArray<a>(l:LazyList<a>):a[] {
	var res = [];
	while(!l.empty) {
		l.destruct((x,xs) => {
			res.push(x.force());
			l = xs.force();
		});
	}
	return res;
}

// Zipper
class Zipper<a> {
	constructor(public l:LazyList<a>, public val:Lazy<a>, public r:LazyList<a>){}
}

// Zipper functor
function z_fmap<a, b>(f:(x:a)=>b, z:Zipper<a>):Zipper<b> {
	return new Zipper(
		ll_fmap(f, z.l),
		l_fmap(f, z.val),
		ll_fmap(f, z.r)
	);
}

// Zipper comonad
function cojoin<a>(z:Zipper<a>):Zipper<Zipper<a>> {
	return new Zipper<Zipper<a>>(
		tail(iterate(left, z)),
		defer(z),
		tail(iterate(right, z))
	);
}

var coreturn:<a>(z:Zipper<a>)=>a = head;

function cobind<a, b>(z:Zipper<a>, f:(zz:Zipper<a>)=>b):Zipper<b> {
	return z_fmap(f, cojoin(z));
}


// Zipper utils
function left<a>(z:Zipper<a>):Zipper<a> {
	if(z.l.empty) {
		return z;
	}

	var nv:Lazy<a>;
	var nl:LazyList<a>;

	z.l.destruct((x, xs) => {
		nv = x;
		nl = xs.force();
	});

	return new Zipper(nl, nv, new LazyCons(z.val, defer(z.r)));
}

function right<a>(z:Zipper<a>):Zipper<a> {
	if(z.r.empty) {
		return z;
	}

	var nv:Lazy<a>;
	var nr:LazyList<a>;

	z.r.destruct((x, xs) => {
		nv = x;
		nr = xs.force();
	});

	return new Zipper(new LazyCons(z.val, defer(z.l)), nv, nr);
}

function head<a>(z:Zipper<a>):a {
	return z.val.force();
}

// Utils
function toNum(b0:boolean, b1:boolean, b2:boolean):number {
	return (b0?1:0) + (b1?2:0) + (b2?4:0);
}

function applyRule(rule:boolean[]):(z:Zipper<boolean>)=>boolean {
	return z => {
		var l = head(left(z));
		var c = head(z);
		var r = head(right(z));
		var n = toNum(r, c, l);
		return rule[n];
	};
}

function toBin(n:number):boolean[] {
	var r = [];
	for(var i = 0; i<8; i++) {
		r.push(((n & Math.pow(2, i)) >>> i) === 1);	
	}
	return r;
}

function toStr(z:Zipper<boolean>):Zipper<string> {
	return z_fmap(function (b) { return b ? '#' : ' '; }, z);
}

var pre = document.body.querySelector('pre');
function log<a>(n:number, z:Zipper<a>) {
	var l = toArray(take(n, z.l)).reverse().join('');
    var c = head(z);
    var r = toArray(take(n, z.r)).join('');
    pre.appendChild(document.createTextNode(l + c + r + '\n'));
}

// main
var rule = applyRule(toBin(90));

var z0 = new Zipper(repeat(false), defer(true), repeat(false));
var zs = iterate(function(z) { return cobind(z, rule); }, z0);

toArray(take(200, zs)).forEach(function(z) {
	log(100, toStr(z));
});