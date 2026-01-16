let cache = {};
const stats = { hits: 0, misses: 0, sets: 0, removes: 0 };

const get = function(key) {
	const val = cache[key];
	//if not exists, or (value has expires and its in the past)
	if (!val || (val.expires > 0 && val.expires < new Date().getTime())) {
		stats.misses++;
		return null;
	}
  stats.hits++;
	return val.value
}
const set = function(key, value, ttl) {
	stats.sets++;
	cache[key] = {
		value,
		expires: ttl > 0 ? new Date().getTime() + ttl : -1
	};
}
const remove = function(key) {
	stats.removes++;
	delete cache[key]
}
const removeAll = function() {
	cache = {};
}

const getStats = function() {
	return stats;
}

const memo = async function(key, fn, ttl, forceUpdate) {
	const value = get(key);
	if (!forceUpdate && value) {
		return value;
	}
	const result = await fn();
	set(key, result, ttl);
	return result;
}

export {
  get,
  set,
  remove,
  removeAll,
  getStats,
  memo
};