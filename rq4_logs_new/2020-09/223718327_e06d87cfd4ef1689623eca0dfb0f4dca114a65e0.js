// counting sort is a stable sorting algorithms
// 计数排序是一个稳定排序。
// 计数排序不是比较排序，因此不被 O(nlog(n)) 的下界限制。

module.exports.counting_sort = arr => {
  // js version
  const c = [];
  const d = [];

  for (let i = 0; i < arr.length; i++) {
    const j = arr[i];
    if (c[j]) {
      c[j]++;
    } else c[j] = 1;
  }

  for (let i = 0; i < c.length; i++) {
    if (c[i]) {
      while (c[i] > 0) {
        d.push(i);
        c[i]--;
      }
    }
  }

  return d;
};

module.exports.counting_sort_common = arr => {
  // common version
  const c = [];
  const d = [];

  for (let i = 0; i < arr.length; i++) {
    const j = arr[i];
    if (c[j]) {
      c[j]++;
    } else c[j] = 1;
  }


  for (let i = 1; i < c.length; i++) {
    c[i - 1] = c[i - 1] || 0;
    c[i] = c[i - 1] + (c[i] || 0);
  }

  c
  for (let i = arr.length - 1; i >= 0; i--) {
    const target = arr[i];
    const rank = c[target];
    d[rank] = arr[i];
    c[target] = rank - 1;
  }

  d.shift()
  return d;
};