var safeLength = 15; // 14 ?
const add1 = (x, y) => {
    var sum = ''; // ��ӽ��
    var flag = 0; // ��λ
    while (x.length > 0 || y.length > 0) {
        // ���������safeLength�����ֵ����ֵ
        var tailX = x.substr(-safeLength) || '0';
        var tailY = y.substr(-safeLength) || '0';
        var tailSum = (parseInt(tailX) + parseInt(tailY) + flag).toString();
        // ǰͷ����0���պ��к���0��ͷ�������
        var orginLength = Math.max(tailX.length, tailY.length);
        if (tailSum.length < orginLength) {
            tailSum = '0'.repeat(orginLength - tailSum.length) + tailSum;
        }
        // ���¼����λ
        flag = parseInt(tailSum.substr(0, tailSum.length - safeLength) || 0);
        // ����λ֮�⣬���ӵ����ǰ��
        sum = tailSum.substr(-safeLength) + sum;
        // ȥ���Ѽ���������safeLength������
        x = x.substr(0, x.length - safeLength);
        y = y.substr(0, y.length - safeLength);
    }
    // ���Ľ�λ
    sum = flag ? flag + sum : sum;
    return sum;
}

function add2(a, b) {
    var aList = a.split('').reverse();
    var bList = b.split('').reverse();
    var max = Math.max(aList.length, bList.length);
    var cList = [];
    var flag = 0;
    
    for (var i = 0; i < max; i++) {
        var temp = (+aList[i] || 0) + (+bList[i] || 0) + flag;
        flag = 0;
        if (temp > 9) {
            temp -= 10;
            flag = 1;
        }
        cList.push(temp);
    }
    if (flag > 0) {
        cList.push(flag);
    }
    return cList.reverse().join('');
}

(function test() {
    let times = 100;
    let numbers = '';
    while(--times) {
        numbers += Math.random();
    }
    numbers = numbers.replace(/\D/g, '');
      
    let counts = 1e5;
    while(--counts) {
        let x = numbers.substring(Math.random() * 10 >> 0, Math.random() * 30 >> 0);
        let y = numbers.substring(Math.random() * 30 >> 0, Math.random() * 80 >> 0);
        //x = '1736';
        //y = '611111750505538122824820966007687248173708494041043351';
        var sum1 = add1(x, y);
        var sum2 = add2(x, y);
        if (sum1 != sum2) {
            console.log('x:' + x);
            console.log('y:' + y);
            console.log('sum1', sum1);
            console.log('sum2', sum2);
        }
    }
})()