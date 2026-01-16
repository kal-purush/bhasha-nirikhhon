class Functional
{
}


export function debounce<T extends Function>(callback:T, wait:number, immediate:boolean = false):any
{
	var timeout:number = -1;
	return function(...args:Array<any>)
	{
		var self = this;

		var callbackLater = function()
		{
			timeout = -1;
			callback.apply(self, args);
		};

		var now = immediate && !timeout;
		clearTimeout(timeout);
		timeout = setTimeout(callbackLater, wait);

		if(now)
		{
			callback.apply(self, args);
		}
	};
}

export function throttle<T extends Function>(callback:T, threshhold:number, scope:any)
{
	var last:number, timer:number = -1;
	return function(...args:Array<any>)
	{
		var context = scope || this;
		var now = +new Date();

		if(last && now < last + threshhold)
		{
			clearTimeout(timer);
			timer = setTimeout(function()
			{
				last = now;
				callback.apply(context, args);
			}, threshhold);
		}
		else
		{
			last = now;
			callback.apply(context, args);
		}
	};
}


export function once<T extends Function>(callback:T, scope:any = null)
{
	var called:boolean = false;
	return function()
	{
		if(!called)
		{
			called = true;
			callback.apply(scope, arguments);
		}
	};
}

