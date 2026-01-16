document.addEventListener('DOMContentLoaded', function() {
    // 模拟博客文章数据
    const posts = [
        { title: '前端小项目展示', summary: '点进去看就是了' },
        { title: 'ROS', summary: '只有命令，没有说明' },
        { title: '单片机', summary: '暂无' }
    ];

    // 动态加载最新文章
    const postList = document.getElementById('post-list');
    posts.forEach(post => {
        const article = document.createElement('article');
        article.innerHTML = `
            <h3>${post.title}</h3>
            <p>${post.summary}</p>
            ${post.title === '前端小项目展示' ? `
                <div class="button-group">
                    <button id="light-control-btn">自由控制灯光</button>
                    <button id="clock-btn">时钟</button>
                    <button id="custom-checkbox-btn">自定义复选框</button>
                    <button id="text-blink-btn">文字闪烁加载</button>
                    <button id="digital-clock-btn">数字时钟</button>
                    <button id="fireworks-btn">烟花大炮</button>
                    <button id="catch-butterfly-btn">抓住这只蝴蝶</button>
                    <button id="spooky-btn">让它变得阴森恐怖</button>
                    <button id="love-confession-btn">七夕表白</button>
                    <button id="switch-btn">开关按钮</button>
                    <button id="heart-loading-btn">爱心跳动加载</button>
                    <button id="responsive-sidebar-btn">响应式侧边栏菜单</button>
                    <button id="panda-login-btn">熊猫登录表单</button>
                    <button id="image-carousel-btn">图片轮播卡片</button>
                    <button id="custom-dropdown-btn">自定义下拉菜单</button>
                    <button id="3d-carousel-btn">3D旋转轮播图</button>
                    <button id="progress-bar-btn">动态百分比进度条</button>
                    <button id="vertical-carousel-btn">垂直轮播</button>
                    <button id="apple-message-btn">苹果消息折叠效果</button>
                    <button id="day-night-switch-btn">日月模式切换</button>
                    <button id="dynamic-squid-btn">充满趣味的动态乌贼</button>
                    <button id="scene-camera-btn">场景相机</button>
                    <button id="lantern-lighting-btn">灯笼点灯</button>
                    <button id="love-letter-btn">表白信封</button>
                </div>
            ` : ''}
            ${post.title === 'ROS' ? `
                <div class="button-group">
                    <button id="ros-install-btn">ros安装</button>
                    <button id="ros-command-btn">ros命令</button>
                    <button id="ros-python-btn">ros的第一个python程序命令</button>
                </div>
            ` : ''}
        `;
        postList.appendChild(article);
    });

    // 加载1.md
    fetch('1.md')
        .then(response => response.text())
        .then(data => {
            console.log('python:', data); 
            const mdContent = document.getElementById('md-content');
            if (mdContent) {
                const parsedContent = marked.parse(data);
                console.log('解析后的内容:', parsedContent);
                mdContent.innerHTML = parsedContent;
            } else {
                console.error('未找到id为md-content的元素');
            }
        })
        .catch(error => {
            console.error('加载1.md失败:', error);
            const mdContent = document.getElementById('md-content');
            if (mdContent) {
                mdContent.innerHTML = '<p>加载内容失败，请稍后再试。</p>';
            }
        });

    // 添加简单的导航功能
    const navLinks = document.querySelectorAll('nav a');
    navLinks.forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const targetId = this.getAttribute('href').substring(1);
            const targetElement = document.getElementById(targetId);
            if (targetElement) {
                targetElement.scrollIntoView({ behavior: 'smooth' });
            }
        });
    });

    // 修改"关于我"文字效果和灯光效果
    const aboutText = document.getElementById('about-text');
    const lightEffect = document.querySelector('.light-effect');
    const originalText = "总有山不青 总有月不圆\n别忘了你是为自己而活";
    let currentIndex = 0;
    let isTyping = true;

    const colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8', '#F06292', '#7986CB', '#9CCC65', '#FFD54F', '#4DB6AC'];
    let colorIndex = 0;

    function typeText() {
        if (isTyping) {
            if (currentIndex < originalText.length) {
                aboutText.innerHTML += originalText.charAt(currentIndex);
                currentIndex++;
                setTimeout(typeText, 200); // 调整打字速度
            } else {
                isTyping = false;
                changeColor();
            }
        }
    }

    function changeColor() {
        const color = colors[colorIndex];
        aboutText.style.color = color;
        lightEffect.style.backgroundColor = color;
        colorIndex = (colorIndex + 1) % colors.length;
        setTimeout(changeColor, 1000); // 每1秒改变一次颜色
    }

    typeText();

    // 修改笑脸动画和文字打字效果
    const smileyOverlay = document.getElementById('smiley-overlay');
    const smileyText = document.querySelector('.smiley-text');
    const happyText = "每天都要开心哦！";
    let textIndex = 0;

    function typeHappyText() {
        if (textIndex < happyText.length) {
            smileyText.textContent += happyText[textIndex];
            textIndex++;
            setTimeout(typeHappyText, 150); // 稍微加快打字速度
        } else {
            // 文字打完后，立即隐藏笑脸
            smileyOverlay.classList.add('hide');
        }
    }

    // 开始打字效果
    typeHappyText();

    // 添加关于模态窗口功能
    const aboutLink = document.querySelector('nav a[href="#about"]');
    const modal = document.getElementById('about-modal');
    const closeBtn = document.getElementById('close-modal-btn');
    const contactBtn = document.getElementById('contact-btn');
    const donateBtnModal = document.getElementById('donate-btn-modal');
    const contactModal = document.getElementById('contact-modal');
    const donateModal = document.getElementById('donate-modal');

    aboutLink.addEventListener('click', function(e) {
        e.preventDefault();
        modal.style.display = 'block';
    });

    closeBtn.addEventListener('click', function() {
        modal.style.display = 'none';
    });

    contactBtn.addEventListener('click', function() {
        modal.style.display = 'none';
        contactModal.style.display = 'block';
    });

    donateBtnModal.addEventListener('click', function() {
        modal.style.display = 'none';
        donateModal.style.display = 'block';
    });

    window.addEventListener('click', function(e) {
        if (e.target == modal) {
            modal.style.display = 'none';
        }
    });

    // 添加联系模态窗口功能
    const contactLink = document.querySelector('nav a[href="#contact"]');
    const closeContactBtn = document.getElementById('close-contact-modal-btn');
    const moreContactBtn = document.getElementById('more-contact-btn');

    contactLink.addEventListener('click', function(e) {
        e.preventDefault();
        contactModal.style.display = 'block';
    });

    closeContactBtn.addEventListener('click', function() {
        contactModal.style.display = 'none';
    });

    moreContactBtn.addEventListener('click', function() {
        window.location.href = '2.html';
    });

    window.addEventListener('click', function(e) {
        if (e.target == contactModal) {
            contactModal.style.display = 'none';
        }
    });

    // 添加赞赏模态窗口功能
    const donateLink = document.querySelector('nav a[href="#donate"]');
    const closeDonateBtn = document.getElementById('close-donate-modal-btn');

    donateLink.addEventListener('click', function(e) {
        e.preventDefault();
        donateModal.style.display = 'block';
    });

    closeDonateBtn.addEventListener('click', function() {
        donateModal.style.display = 'none';
    });

    window.addEventListener('click', function(e) {
        if (e.target == donateModal) {
            donateModal.style.display = 'none';
        }
    });

    // 获取随机文案并显示在My blog下面
    function generateQuotes() {
        const apiUrl = 'https://v1.hitokoto.cn/';

        return fetch(apiUrl)
            .then(response => response.json())
            .then(data => {
                const quote = data.hitokoto;
                return quote;
            })
            .catch(error => {
                console.error('随机文案获取失败:', error);
                return "获取随机文案失败";
            });
    }

    const randomQuoteElement = document.getElementById('random-quote');
    function updateQuote() {
        generateQuotes().then(quote => {
            randomQuoteElement.textContent = quote;
        });
    }

    // 初始获取随机文案
    updateQuote();

    // 每隔10秒更新一次随机文案
    setInterval(updateQuote, 10000);

    // JavaScript代码用于控制灯光
    document.getElementById('light-switch').addEventListener('change', function() {
        if (this.checked) {
            document.body.style.backgroundColor = 'white';
        } else {
            document.body.style.backgroundColor = 'black';
        }
    });

    // 添加跳转到自由控制灯光页面的按钮点击事件
    document.getElementById('light-control-btn').addEventListener('click', function() {
        window.location.href = '自由控制灯光.html';
    });

    // 添加跳转到时钟页面的按钮点击事件
    document.getElementById('clock-btn').addEventListener('click', function() {
        window.location.href = '时钟.html';
    });

    // 添加跳转到自定义复选框页面的按钮点击事件
    document.getElementById('custom-checkbox-btn').addEventListener('click', function() {
        window.location.href = '自定义复选框.html';
    });

    // 添加跳转到文字闪烁加载页面的按钮点击事件
    document.getElementById('text-blink-btn').addEventListener('click', function() {
        window.location.href = '文字闪烁加载.html';
    });

    // 添加跳转到数字时钟页面的按钮点击事件
    document.getElementById('digital-clock-btn').addEventListener('click', function() {
        window.location.href = '数字时钟.html';
    });

    // 添加跳转到烟花大炮页面的按钮点击事件
    document.getElementById('fireworks-btn').addEventListener('click', function() {
        window.location.href = '烟花大炮.html';
    });

    // 添加跳转到抓住这只蝴蝶页面的按钮点击事件
    document.getElementById('catch-butterfly-btn').addEventListener('click', function() {
        window.location.href = '抓住这只蝴蝶.html';
    });

    // 添加跳转到让它变得阴森恐怖页面的按钮点击事件
    document.getElementById('spooky-btn').addEventListener('click', function() {
        window.location.href = '让它变得阴森恐怖.html';
    });

    // 添加跳转到七夕表白按钮页面的按钮点击事件
    document.getElementById('love-confession-btn').addEventListener('click', function() {
        window.location.href = '七夕表白按钮.html';
    });

    // 添加跳转到ros安装页面的按钮点击事件
    document.getElementById('ros-install-btn').addEventListener('click', function() {
        window.location.href = 'ros安装.html';
    });

    // 添加跳转到ros命令页面的按钮点击事件
    document.getElementById('ros-command-btn').addEventListener('click', function() {
        window.location.href = 'ros命令.html';
    });

    // 添加跳转到ros的第一个python程序命令页面的按钮点击事件
    document.getElementById('ros-python-btn').addEventListener('click', function() {
        window.location.href = 'ros的第一个python程序命令.html';
    });

    // 添加新的按钮点击事件
    const newButtons = [
        { id: 'switch-btn', url: '开关按钮.html' },
        { id: 'heart-loading-btn', url: '爱心跳动加载.html' },
        { id: 'responsive-sidebar-btn', url: '响应式侧边栏菜单.html' },
        { id: 'panda-login-btn', url: '熊猫登录表单.html' },
        { id: 'image-carousel-btn', url: '图片轮播卡片.html' },
        { id: 'custom-dropdown-btn', url: '自定义下拉菜单.html' },
        { id: '3d-carousel-btn', url: '3D旋转轮播图.html' },
        { id: 'progress-bar-btn', url: '动态百分比进度条.html' },
        { id: 'vertical-carousel-btn', url: '垂直轮播.html' },
        { id: 'apple-message-btn', url: '苹果消息折叠效果.html' },
        { id: 'day-night-switch-btn', url: '日月模式切换.html' },
        { id: 'dynamic-squid-btn', url: '充满趣味的动态乌贼.html' },
        { id: 'scene-camera-btn', url: '场景相机.html' },
        { id: 'lantern-lighting-btn', url: '灯笼点灯.html' },
        { id: 'love-letter-btn', url: '表白信封.html' },

    ];

    newButtons.forEach(button => {
        document.getElementById(button.id).addEventListener('click', function() {
            window.location.href = button.url;
        });
    });

    // 更新网站资讯
    function updateSiteInfo() {
        // 文章数目（这里假设文章数量等于 posts 数组的长度）
        document.getElementById('article-count').textContent = posts.length;

        // 已运行时间（假设网站创建日期为 2024 年 9 月 24 日）
        const startDate = new Date('2024-09-24');
        const now = new Date();
        const runDays = Math.floor((now - startDate) / (1000 * 60 * 60 * 24));
        document.getElementById('run-time').textContent = runDays + ' 天';

        // 获取访客数和访问量
        fetch('/api/site-stats')
            .then(response => response.json())
            .then(data => {
                document.getElementById('visitor-count').textContent = data.visitorCount;
                document.getElementById('visit-count').textContent = data.visitCount;
            })
            .catch(error => {
                console.error('获取网站统计数据失败:', error);
                document.getElementById('visitor-count').textContent = '获取失败';
                document.getElementById('visit-count').textContent = '获取失败';
            });

        // 最后更新时间（固定时间）
        document.getElementById('last-update').textContent = '2024年9月27日';
    }

    // 在页面加载完成后调用更新函数
    updateSiteInfo();

    // 每分钟更新一次网站资讯（除了最后更新时间）
    setInterval(updateSiteInfo, 60000);
});
const root=document.documentElement;
const dropdown_title_icon=document.querySelector(".dropdown-title-icon");
const dropdown_title=document.querySelector(".dropdown-title");
const dropdown_list=document.querySelector(".dropdown-list");
const main_button=document.querySelector(".main-button");
const floating_icon=document.querySelector(".floating-icon");

const icons={
    "百度":
        "M226.8 535.7c96.8-20.8 83.6-136.4 80.6-161.7-4.8-39-50.6-107.2-112.8-101.8-78.3 7-89.8 120.2-89.8 120.2-10.5 52.3 25.4 164.1 122 143.3z m102.7 201.1c-2.9 8.2-9.1 28.9-3.7 47 10.8 40.6 46.1 42.4 46.1 42.4h50.7V702.4h-54.2c-24.5 7.3-36.3 26.3-38.9 34.4z m76.8-395c53.5 0 96.6-61.5 96.6-137.6 0-76-43.1-137.5-96.6-137.5-53.4 0-96.6 61.5-96.6 137.5 0 76.1 43.2 137.6 96.6 137.6z m230.2 9.1c71.4 9.3 117.4-67 126.4-124.8 9.3-57.6-36.8-124.7-87.3-136.2-50.7-11.6-113.9 69.5-119.7 122.4-6.8 64.8 9.3 129.4 80.6 138.6z m175 339.6S701 605 636.5 512.6c-87.4-136.3-211.7-80.8-253.2-11.6-41.4 69.3-105.8 113.1-115 124.7-9.3 11.5-133.5 78.5-105.9 200.9C189.9 949 286.8 946.8 286.8 946.8s71.3 7 154.2-11.5c82.8-18.4 154.1 4.6 154.1 4.6s193.4 64.7 246.4-60c52.8-124.8-30-189.4-30-189.4z m-331 185.6H354.8c-54.3-10.9-76-47.9-78.7-54.2-2.7-6.4-18.1-36.2-9.9-86.9 23.5-76 90.4-81.4 90.4-81.4h66.9v-82.3l57 0.9v303.9z m234.3-0.9H570c-56.1-14.4-58.7-54.3-58.7-54.3v-160l58.7-1v143.8c3.6 15.3 22.7 18.2 22.7 18.2h59.6v-161h62.5v214.3z m204.8-427.3c0-27.6-22.9-110.9-108.1-110.9-85.3 0-96.7 78.6-96.7 134.1 0 53 4.5 127.1 110.5 124.6 106-2.3 94.3-120 94.3-147.8z m0 0",
    "微信":
        "M308.73856 119.23456C23.65696 170.15296-71.37024 492.23936 155.392 639.66464c12.43392 7.99232 12.43392 7.104-6.21824 62.76096l-15.98464 47.65952 57.43104-30.784 57.43104-30.78656 30.49216 7.40096c31.96928 7.99232 72.82432 13.61664 100.0576 13.61664l16.28416 0-5.62688-21.61152c-44.70016-164.5952 109.82912-327.71072 310.8352-327.71072l27.2384 0-5.62432-19.53792C677.59616 186.43456 491.392 86.67136 308.73856 119.23456zM283.87072 263.40352c30.1952 20.4288 31.97184 64.5376 2.95936 83.48416-47.06816 30.78656-102.1312-23.38816-70.45632-69.57056C230.28736 256.59648 263.74144 249.78688 283.87072 263.40352zM526.62016 263.40352c49.73568 33.45408 12.43392 110.71744-43.22304 89.40288-40.25856-15.39328-44.99712-70.75072-7.40096-90.5856C490.79808 254.22848 513.88928 254.81984 526.62016 263.40352zM636.44928 385.37216c-141.2096 25.7536-239.19872 132.91776-233.57184 256.06656 7.40096 164.89472 200.71168 278.56896 386.32448 227.65312l21.90592-5.92128 46.1824 24.8704c25.4592 13.9136 46.77376 23.97696 47.36512 22.79168 0.59392-1.47968-4.43648-19.24352-10.95168-39.6672-14.79936-45.59104-15.09632-42.33472 4.73856-56.54272C1121.64864 654.464 925.67552 332.97408 636.44928 385.37216zM630.82496 518.28992c12.4288 8.28928 18.944 29.01248 13.61408 44.1088-11.24864 32.26624-59.49952 34.63424-72.52992 3.55328C557.10976 530.13248 597.9648 496.97536 630.82496 518.28992zM828.57472 521.84576c19.53792 18.64704 16.2816 50.32448-6.51264 62.16448-34.93376 17.76128-71.63904-17.76128-53.58336-51.80416C780.32128 510.2976 810.81344 504.97024 828.57472 521.84576z",
    "抖音":
        "M937.4 423.9c-84 0-165.7-27.3-232.9-77.8v352.3c0 179.9-138.6 325.6-309.6 325.6S85.3 878.3 85.3 698.4c0-179.9 138.6-325.6 309.6-325.6 17.1 0 33.7 1.5 49.9 4.3v186.6c-15.5-6.1-32-9.2-48.6-9.2-76.3 0-138.2 65-138.2 145.3 0 80.2 61.9 145.3 138.2 145.3 76.2 0 138.1-65.1 138.1-145.3V0H707c0 134.5 103.7 243.5 231.6 243.5v180.3l-1.2 0.1",
    "哔哩哔哩":
        "M306.005333 117.632L444.330667 256h135.296l138.368-138.325333a42.666667 42.666667 0 0 1 60.373333 60.373333L700.330667 256H789.333333A149.333333 149.333333 0 0 1 938.666667 405.333333v341.333334a149.333333 149.333333 0 0 1-149.333334 149.333333h-554.666666A149.333333 149.333333 0 0 1 85.333333 746.666667v-341.333334A149.333333 149.333333 0 0 1 234.666667 256h88.96L245.632 177.962667a42.666667 42.666667 0 0 1 60.373333-60.373334zM789.333333 341.333333h-554.666666a64 64 0 0 0-63.701334 57.856L170.666667 405.333333v341.333334a64 64 0 0 0 57.856 63.701333L234.666667 810.666667h554.666666a64 64 0 0 0 63.701334-57.856L853.333333 746.666667v-341.333334A64 64 0 0 0 789.333333 341.333333zM341.333333 469.333333a42.666667 42.666667 0 0 1 42.666667 42.666667v85.333333a42.666667 42.666667 0 0 1-85.333333 0v-85.333333a42.666667 42.666667 0 0 1 42.666666-42.666667z m341.333334 0a42.666667 42.666667 0 0 1 42.666666 42.666667v85.333333a42.666667 42.666667 0 0 1-85.333333 0v-85.333333a42.666667 42.666667 0 0 1 42.666667-42.666667z",
    "淘宝":
        "M168.5 273.7a68.7 68.7 0 1 0 137.4 0 68.7 68.7 0 1 0-137.4 0z m730 79.2s-23.7-184.4-426.9-70.1c17.3-30 25.6-49.5 25.6-49.5L396.4 205s-40.6 132.6-113 194.4c0 0 70.1 40.6 69.4 39.4 20.1-20.1 38.2-40.6 53.7-60.4 16.1-7 31.5-13.6 46.7-19.8-18.6 33.5-48.7 83.8-78.8 115.6l42.4 37s28.8-27.7 60.4-61.2h36v61.8H372.9v49.5h140.3v118.5c-1.7 0-3.6 0-5.4-0.2-15.4-0.7-39.5-3.3-49-18.2-11.5-18.1-3-51.5-2.4-71.9h-97l-3.4 1.8s-35.5 159.1 102.3 155.5c129.1 3.6 203-36 238.6-63.1l14.2 52.6 79.6-33.2-53.9-131.9-64.6 20.1 12.1 45.2c-16.6 12.4-35.6 21.7-56.2 28.4V561.3h137.1v-49.5H628.1V450h137.6v-49.5H521.3c17.6-21.4 31.5-41.1 35-53.6l-42.5-11.6c182.8-65.5 284.5-54.2 283.6 53.2v282.8s10.8 97.1-100.4 90.1l-60.2-12.9-14.2 57.1S882.5 880 903.7 680.2c21.3-200-5.2-327.3-5.2-327.3z m-707.4 18.3l-45.4 69.7 83.6 52.1s56 28.5 29.4 81.9C233.8 625.5 112 736.3 112 736.3l109 68.1c75.4-163.7 70.5-142 89.5-200.7 19.5-60.1 23.7-105.9-9.4-139.1-42.4-42.6-47-46.6-110-93.4z"
};
const list_items=["百度","微信","抖音","哔哩哔哩","淘宝"];


const iconTemplate=(path)=>{
    return `
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1024 1024">
            <path d="${path}"></path>
        </svg>
    `;
}

const listItemTemplate=(text,translate_value)=>{
    return `
        <li class="dropdown-list-item">
            <button class="dropdown-button list-button" data-translate-value="${translate_value}%">
                <span class="text-truncate">${text}</span>
            </button>
        </li>
    `;
}

const renderListItems=()=>{
    dropdown_list.innerHTML+=list_items.map((item,index)=>{
        return listItemTemplate(item,100*index);
    }).join("");
}

window.addEventListener("load",()=>{
    renderListItems();
})

const setDropdownProps=(deg,ht,opacity)=>{
    root.style.setProperty("--rotate-arrow",deg!==0?deg+"deg":0);
    root.style.setProperty("--dropdown-height",ht!==0?ht+"rem":0);
    root.style.setProperty("--list-opacity",opacity);
}

main_button.addEventListener("click",()=>{
    const list_wrapper_sizes=3.5;
    const dropdown_open_height=4.6*list_items.length+list_wrapper_sizes;
    const curr_dropdown_height=root.style.getPropertyValue("--dropdown-height")||0;
    curr_dropdown_height==="0"?setDropdownProps(180,dropdown_open_height,1):setDropdownProps(0,0,0);
})

dropdown_list.addEventListener("mouseover",(e)=>{
    const translate_value=e.target.dataset.translateValue;
    root.style.setProperty("--translate-value",translate_value);
})

dropdown_list.addEventListener("click",(e)=>{
    const clicked_item_text=e.target.innerText.toLowerCase().trim();
    const clicked_item_icon=icons[clicked_item_text];
    dropdown_title_icon.innerHTML=iconTemplate(clicked_item_icon);
    dropdown_title.innerHTML=clicked_item_text;
    setDropdownProps(0,0,0);
})

dropdown_list.addEventListener("mousemove",(e)=>{
    const icon_size=root.style.getPropertyValue("--floating-icon-size")||0;
    const x=e.clientX-dropdown_list.getBoundingClientRect().x;
    const y=e.clientY-dropdown_list.getBoundingClientRect().y;
    const targetText=e.target.innerText.toLowerCase().trim();
    const hover_item_text=icons[targetText];
    floating_icon.innerHTML=iconTemplate(hover_item_text);
    root.style.setProperty("--floating-icon-left",x-icon_size/2+"px");
    root.style.setProperty("--floating-icon-top",y-icon_size/2+"px");
})
window.addEventListener("DOMContentLoaded", () => {
	const c = new Clock28(".clock");
});

class Clock28 {
	activeClass = "clock__unit--active";
	pristineClass = "clock__unit--pristine";
	lastTime = null;

	constructor(el) {
		this.el = document.querySelector(el);

		this.init();
	}
	init() {
		this.makePristine();
		this.timeUpdate();
	}
	get timeAsObject() {
		const date = new Date();
		const h = Utils.digits(date.getHours());
		const m = Utils.digits(date.getMinutes());
		const s = Utils.digits(date.getSeconds());

		return { h, m, s };
	}
	get timeAsString() {
		const { h, m, s } = this.timeAsObject;

		return [h, m, s].join(":");
	}
	makePristine() {
		// clear animations
		const unitEls = Array.from(this.el?.querySelectorAll('[data-unit]'));
		for (let unitEl of unitEls) {
			unitEl.classList.add(this.pristineClass);
		}
	}
	removeAnimations() {
		// clear animations
		const unitEls = Array.from(this.el?.querySelectorAll('[data-unit]'));
		for (let unitEl of unitEls) {
			unitEl.classList.remove(this.activeClass, this.pristineClass);
		}
	}
	timeUpdate() {
		// update the `aria-label`
		this.el?.setAttribute("aria-label", this.timeAsString);
		// update the units
		const time = this.timeAsObject;
		for (let unit in time) {
			const unitEl = this.el?.querySelector(`[data-unit="${unit}"]`);
			const prev = Array.from(unitEl?.querySelectorAll("[data-prev]"));
			const next = Array.from(unitEl?.querySelectorAll("[data-next]"));

			for (let p of prev) {
				let prevDigits = +time[unit] - 1;
				if (prevDigits < 0) prevDigits += 60;

				p.innerText = Utils.digits(prevDigits);
			}
			for (let n of next) {
				n.innerText = time[unit];
			}
			// animate the flip
			if (+time[unit] !== +this.lastTime?.[unit]) {
				unitEl.classList.add(this.activeClass);
			}
		}
		this.lastTime = time;
		// loop
		clearTimeout(this.animationLoop);
		this.animationLoop = setTimeout(this.removeAnimations.bind(this), 500);
		clearTimeout(this.timeUpdateLoop);
		this.timeUpdateLoop = setTimeout(this.timeUpdate.bind(this), 1e3);
	}
}
class Utils {
	static digits(n) {
		if (n < 10) return `0${n}`;
		return `${n}`;
	}
}
/*** Uncomment the code below to get a "LAUNCH ALL" button ***/
/*** Challenge proposed by PX, see the comments in Detail View ***/

/*
var x = document.createElement("button"); 
document.body.appendChild(x); 
function dqs(x){return document.querySelector(x);} 
dqs('button').setAttribute('id', 'launch-all'); 
dqs('button').innerText='LAUNCH ALL'; 
dqs('#launch-all').addEventListener('click', function() { 
	setTimeout(() => { dqs('#check-0').checked = true; }, 100); 
	setTimeout(() => { dqs('#check-1').checked = true; }, 300); 
	setTimeout(() => { dqs('#check-2').checked = true; }, 500); 
	setTimeout(() => { dqs('#check-3').checked = true; }, 700); 
	setTimeout(() => { dqs('#check-4').checked = true; }, 900); 
	setTimeout(() => { dqs('#check-5').checked = true; }, 1100); 
	setTimeout(() => { dqs('#check-6').checked = true; }, 1200); 
	setTimeout(() => { dqs('#check-7').checked = true; }, 1000); 
	setTimeout(() => { dqs('#check-8').checked = true; }, 800); 
	setTimeout(() => { dqs('#check-9').checked = true; }, 600); 
	setTimeout(() => { dqs('#check-10').checked = true; }, 400); 
	setTimeout(() => { dqs('#check-11').checked = true; }, 200); 
});
*/
checkTransform = -15;
function flyFunc() {
  wiggleFunc();
  hh = Math.random() * (document.getElementById("borderDiv").offsetHeight - document.getElementById("borderDiv").offsetTop);
  ww = Math.random() * (document.getElementById("borderDiv").offsetWidth - document.getElementById("borderDiv").offsetLeft);
  //console.log(hh+" , "+ww);
  hh += document.getElementById("borderDiv").offsetTop;
  ww += document.getElementById("borderDiv").offsetLeft;
  //console.log(Math.atan2(hh - document.getElementById("butterWrapper").offsetTop, ww - document.getElementById("butterWrapper").offsetLeft))
  checkTransform = (Math.atan2(hh - document.getElementById("butterWrapper").offsetTop, ww - document.getElementById("butterWrapper").offsetLeft) * (180 / Math.PI));
  document.getElementById("butterWrapper").style.transform = "rotate(" + (checkTransform + 80) + "deg)";
  document.getElementById("butterWrapper").style.top = hh + "px";
  document.getElementById("butterWrapper").style.left = ww + "px";
  //console.log("H: "+document.getElementById("borderDiv").offsetHeight);
  //console.log("W: "+document.getElementById("borderDiv").offsetWidth);
  document.getElementById("butterWrapper").style.pointerEvents = "none";
  document.getElementsByClassName("shade")[0].style.animationName = "none";
  document.getElementsByClassName("shade")[0].style.animationPlayState = "running";
  var x = document.getElementsByClassName("wing");
  var i;
  for (i = 0; i < x.length; i++) {
    x[i].style.animationName = "none";
    x[i].style.animationPlayState = "running";
  }
  setTimeout(function () {
    document.getElementsByClassName("shade")[0].style.animationName = "shadeAnim";
    var x = document.getElementsByClassName("wing");
    var i;
    for (i = 0; i < x.length; i++) {
      x[i].style.animationName = "wingAnim";
    }
  }, 50);
  setTimeout(function () {
    clearTimeout(wiggleTime);
    document.getElementById("butterWrapper").style.pointerEvents = "all";
  }, 1500);
}
function wiggleFunc() {
  rl = Math.random() * 40;
  rt = Math.random() * 40;
  document.getElementById("innerWrapper").style.left = rl + "px";
  document.getElementById("innerWrapper").style.top = rt + "px";
  wiggleTime = setTimeout(function () {
    wiggleFunc();
  }, 200);
}
let percent = 20
const percentBox = document.querySelector('#percent')
const circle = document.querySelector('#circle')
const timer = setInterval(() => {
  percent += Math.ceil(Math.random() * 30)
  if (percent > 100) {
    percent = 100
    clearInterval(timer)
  }
  percentBox.innerHTML = percent
  circle.style.strokeDashoffset = `calc(440 - 440 * (${percent} / 100))`
}, 1500)
const textfield = document.getElementById("spookytext"),
	upperteeth = document.getElementById("upper"),
	lowerteeth = document.getElementById("lower"),
	pumpkin = document.getElementById("pumpkin"),
	body = document.body;
let textTotal = 0,
	finished = 0,
	newChar,
	letter,
	key,
	prevChars = [],
	newChars = [],
	charOffset;

textfield.addEventListener("keyup", compareChar);

function compareChar() {
	key = event.keyCode || event.charCode;
	newChars = textfield.value.split("");
	textTotal = newChars.length;
	charOffset = textTotal - prevChars.length;
	if (charOffset > 0) {
		for (i = charOffset; i > 0; i--) {
			let newChar = newChars.slice(i * -1)[0];
			if (newChar !== undefined && key !== 8) {
				letter = document.createElement("span");
				letter.textContent = newChar;
				letter.classList.add("char");
				letter.setAttribute("data-char", newChar);
				letter.style.setProperty("--char-index", textTotal);
				if (textTotal < 9) {
					upperteeth.appendChild(letter);
				} else if (textTotal < 18) {
					lowerteeth.appendChild(letter);
				}
			}
		}
	}
	if (textTotal < 9) {
		upperteeth.style.setProperty("--char-total", textTotal);
		body.style.setProperty("--char-total", textTotal);
	} else if (textTotal < 18) {
		lowerteeth.style.setProperty("--char-total", textTotal - 8);
		body.style.setProperty("--char-total", textTotal - 8);
	}
	if (textTotal > 0) {
		pumpkin.classList.add("open");
	} else {
		pumpkin.classList.remove("open");
		upperteeth.innerHTML = "";
		lowerteeth.innerHTML = "";
	}
	if (key == 8) {
		document.querySelectorAll(".char").forEach((char, i) => {
			if (i === textTotal) {
				char.remove();
			}
		});
	}
	prevChars = newChars;
}
const ul = document.querySelector("ul")
ul.addEventListener('click', () => {
    ul.classList.toggle('active')
})
const sunBtn = document.querySelector('.sunBtn')
const moonBtn = document.querySelector('.moonBtn')

sunBtn.addEventListener('click', function () {
    document.getElementById('container').setAttribute('class', 'light');
})

moonBtn.addEventListener('click', function () {
    document.getElementById('container').setAttribute('class', 'dark');
})
const canvas = document.querySelector('canvas');
const ctx = canvas.getContext('2d');

const PI2 = Math.PI * 2;

const mouse = { x: 0, y: 0, angle: 0 };
const gravity = 0.1;
const friction = 0.95;

let w;
let wH;
let h;
let hH;

const radius = 30;
let squid;
const tentacleWidth = 8;
const numTentacles = 6;
const numPoints = 10;
let particles = [];

let tentacles;

const distanceBetween = (p1, p2) => Math.sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y));
const angleBetween = (x1, y1, x2, y2) => Math.atan2(y2 - y1, x2 - x1);
const randomBetween = (min, max) => ~~((Math.random() * (max - min + 1)) + min);

const onResize = () => {
	w = window.innerWidth;
	h = window.innerHeight;

	wH = w >> 1;
	hH = h >> 1;

	canvas.width = w;
	canvas.height = h;
};

const updateStage = () => {
	onResize();

	mouse.x = wH;
	mouse.y = hH;

	squid = { x: mouse.x, y: mouse.y, radius, bodyWidth: radius * 2, bodyHeight: 30, angle: 0, velocity: 0 };
	tentacles = [];

	let connectionX = squid.x - squid.radius - tentacleWidth;
	const incX = squid.bodyWidth / (numTentacles - 1);

	for (let i = 0; i < numTentacles; i++) {
		const length = randomBetween(5, 20);

		const tentacle = {
			length,
			connections: [],
		};

		let connectionY = squid.y + squid.bodyHeight;

		for (let q = 0; q < numPoints; q++) {
			tentacle.connections.push({
				x: connectionX,
				y: connectionY,
				oldX: connectionX,
				oldY: connectionY,
			});

			connectionY += length;
		}

		connectionX += incX;

		tentacles.push(tentacle);
	}
};

const updatePoints = () => {
	tentacles.forEach((tentacle) => {
		const { connections } = tentacle;

		// update velocity and position of each point
		connections.forEach((point) => {
			const velX = point.x - point.oldX;
			const velY = point.y - point.oldY;

			point.oldX = point.x;
			point.oldY = point.y;

			point.x += velX * friction;
			point.y += velY * friction;

			point.y += gravity;
		});
	});
};

const updateSticks = () => {
	tentacles.forEach((tentacle) => {
		const { length, connections } = tentacle;

		// update the sticks between two points
		for (let i = 0; i < connections.length - 1; i++) {
			const from = connections[i];
			const to = connections[i + 1];

			const dx = to.x - from.x;
			const dy = to.y - from.y;

			const distance = distanceBetween(from, to);
			const difference = length - distance;
			const percent = difference / distance / 2;
			const offsetX = dx * percent;
			const offsetY = dy * percent;

			from.x -= offsetX;
			from.y -= offsetY;

			to.x += offsetX;
			to.y += offsetY;
		}
	});
};

const connectTentacles = () => {
	let x = squid.x - squid.radius + (tentacleWidth / 2);
	let y = squid.y + squid.bodyHeight;
	const posInc = (squid.bodyWidth - tentacleWidth) / (tentacles.length - 1);

	tentacles.forEach((tentacle) => {
		const connector = tentacle.connections[0];

		const angleDiff = angleBetween(squid.x, squid.y, x, y);
		const dx = squid.x - x;
		const dy = squid.y - y;
		const h = Math.sqrt((dx * dx) + (dy * dy));

		connector.x = squid.x + (Math.cos(angleDiff + squid.angle) * h);
		connector.y = squid.y + (Math.sin(angleDiff + squid.angle) * h);

		x += posInc;
	});
};

const drawTentacles = () => {
	tentacles.forEach((tentacle) => {
		const { connections } = tentacle;

		ctx.beginPath();
		ctx.lineWidth = tentacleWidth;
		ctx.lineCap = 'round';
		ctx.lineJoin = 'round';
		ctx.moveTo(connections[0].x, connections[0].y);

		connections.slice(1).forEach((connector) => {
			ctx.lineTo(connector.x, connector.y);
		});

		ctx.stroke();
		ctx.closePath();
	});
};

const updateSquid = () => {
	const newX = squid.x + (mouse.x - squid.x) / 50;
	const newY = squid.y + (mouse.y - squid.y) / 50;
	const velocity = squid.x - newX;

	squid.angle = -velocity * 0.1;
	squid.velocity = velocity;
	squid.x = newX;
	squid.y = newY;
};

const drawSquid = () => {
	// lol vars for eyes
	const eyeXInc = Math.cos(mouse.angle) * 5;
	const eyeYInc = Math.sin(mouse.angle) * 5;

	const eyeXInc2 = Math.cos(mouse.angle) * 10;
	const eyeYInc2 = Math.sin(mouse.angle) * 10;

	ctx.save();
	ctx.translate(squid.x, squid.y);
	ctx.rotate(squid.angle);

	// body
	ctx.beginPath();
	ctx.fillStyle = '#000';
	ctx.lineWidth = 1;
	ctx.rect(-squid.radius, 0, squid.bodyWidth, squid.bodyHeight);
	ctx.fill();
	ctx.closePath();

	// head
	ctx.beginPath();
	ctx.fillStyle = '#000';
	ctx.lineWidth = 1;
	ctx.arc(0, 0, squid.radius, 0, PI2, false);
	ctx.fill();
	ctx.closePath();

	// eyes
	ctx.beginPath();
	ctx.fillStyle = '#fff';
	ctx.arc(-15 + eyeXInc, eyeYInc, 4, 0, PI2, false);
	ctx.fill();
	ctx.closePath();

	ctx.beginPath();
	ctx.fillStyle = '#fff';
	ctx.arc(18 + eyeXInc2, eyeYInc2, 6, 0, PI2, false);
	ctx.fill();
	ctx.closePath();

	ctx.restore();
};

const drawParticles = () => {
	particles.forEach((p) => {
		p.radius *= 1.025;
		p.life *= 0.97;
		p.isDead = p.life <= 0.1;

		p.x += Math.cos(p.angle) * p.velocity;
		p.t += Math.sin(p.angle) * p.velocity;

		ctx.beginPath();
		ctx.fillStyle = `rgba(255, 255, 255, ${p.life})`;
		ctx.arc(p.x, p.y, p.radius, 0, PI2, false);
		ctx.fill();
		ctx.closePath();
	});
	particles = particles.filter(p => !p.isDead);
};

const clear = () => {
	ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
};

const loop = () => {
	clear();

	drawParticles();

	updateSquid();

	updatePoints();
	updateSticks();

	connectTentacles();

	drawTentacles();
	drawSquid();

	if (Math.abs(squid.velocity) > 2 && particles.length < 200) {
		tentacles.forEach((tentacle) => {
			const pos = tentacle.connections[tentacle.connections.length - 1];
			const angle = angleBetween(pos.x, pos.y, mouse.x, mouse.y);

			particles.push({
				x: pos.x,
				y: pos.y,
				life: 1,
				radius: 1,
				isDead: false,
				velocity: randomBetween(1, 3) * 0.5,
				angle: angle,
			});
		});
	}

	requestAnimationFrame(loop);
};


window.addEventListener('resize', onResize);
updateStage();
loop();

const onPointerMove = (e) => {
	const target = (e.touches && e.touches.length) ? e.touches[0] : e;
	const { clientX: x, clientY: y } = target;

	mouse.x = x;
	mouse.y = y;
	mouse.angle = angleBetween(squid.x, squid.y, mouse.x, mouse.y);
};

canvas.addEventListener('mousemove', onPointerMove);
canvas.addEventListener('touchmove', onPointerMove);
var rotateDiv = document.getElementById('rot');
var rotateIcons = document.getElementById('rot-icons');
var clickRotateDiv = document.getElementById('click-rot');
var angle = 0;

clickRotateDiv.onclick = function () {
  angle += 60;
  rotateDiv.style.transform = 'rotate(' + angle + 'deg)';
  rotateIcons.style.transform = 'rotate(' + angle + 'deg)';
};

var step = 2;
var color1 = 'rgba(0,0,0,0.5)';
var color2 = 'rgba(0,0,0,0.1)';

var gradient = ' conic-gradient(';
for (var i = 0; i < 360; i += step) {
  var color = i % (2 * step) === 0 ? color1 : color2;
  gradient += color + ' ' + i + 'deg, ';
}
gradient = gradient.slice(0, -2) + '), rgb(85 93 108)';

rotateDiv.style.background = gradient;


var toggles = document.querySelectorAll('.toggle');
var tempElement = document.querySelector('.temp');

let isAnimating = false; // Add flag to indicate if animation is active

toggles.forEach(function (toggle) {
  toggle.addEventListener('click', function () {
    if (this.classList.contains('active') || isAnimating) { // Check if animation is active
      return;
    }
    toggles.forEach(function (toggle) {
      toggle.classList.remove('active');
    });
    this.classList.add('active');
    var tempValue = parseFloat(tempElement.textContent);
    if (this.id === 'toggle-cel') {
      var celsius = Math.round((tempValue - 32) * 5 / 9);
      tempElement.textContent = celsius + '°C';
    } else if (this.id === 'toggle-far') {
      var fahrenheit = Math.round(tempValue * 9 / 5 + 32);
      tempElement.textContent = fahrenheit + '°F';
    }
  });
});

let currentTempF = 34; // Initialize with the initial temperature in Fahrenheit

// cubic ease in/out function
function easeInOutCubic(t) {
  return t < .5 ? 4 * t * t * t : (t - 1) * (2 * t - 2) * (2 * t - 2) + 1;
}

function changeTemp(element, newTemp) {
  let unit = element.innerHTML.includes("F") ? "°F" : "°C";
  let currentTemp = unit === "°F" ? currentTempF : Math.round((currentTempF - 32) * 5 / 9);
  let finalTemp = unit === "°F" ? newTemp : Math.round((newTemp - 32) * 5 / 9);

  let duration = 2000; // Duration of the animation in milliseconds
  let startTime = null;

  function animate(currentTime) {
    if (startTime === null) {
      startTime = currentTime;
    }

    let elapsed = currentTime - startTime;
    let progress = Math.min(elapsed / duration, 1);
    progress = easeInOutCubic(progress);

    let tempNow = Math.round(currentTemp + (progress * (finalTemp - currentTemp)));
    element.innerHTML = `${tempNow}${unit}`;

    if (progress < 1) {
      requestAnimationFrame(animate);
    } else {
      // Update currentTempF once the animation is complete
      currentTempF = newTemp;
      isAnimating = false; // Reset the flag when animation is done
    }
  }

  isAnimating = true; // Set flag when animation starts
  requestAnimationFrame(animate);
}


window.onload = function () {
  const sixths = Array.from(document.querySelectorAll('.sixths'));
  let index = 0;
  let temp = document.querySelector('.temp');

  document.querySelector('#rot-icons').addEventListener('click', () => {
    sixths[index].classList.remove('active');
    index = (index + 1) % sixths.length;
    sixths[index].classList.add('active');
    if (index == 0) {
      changeTemp(temp, 34);
      console.log("sun")
      document.querySelector('#mountains').classList.remove("snow");
      document.querySelector('#mountains').classList.remove("clouds");
    } else if (index == 1) {
      changeTemp(temp, 27);
      console.log("sunset")
      document.querySelector('#mountains').classList.add("sunset");
    } else if (index == 2) {
      changeTemp(temp, 14);
      console.log("moon")
      document.querySelector('#mountains').classList.remove("sunset");
      document.querySelector('#mountains').classList.add("moon");
    } else if (index == 3) {
      changeTemp(temp, 16);
      console.log("clouds")
      document.querySelector('#mountains').classList.add("clouds");
    } else if (index == 4) {
      changeTemp(temp, 8);
      console.log("storm")
      document.querySelector('#mountains').classList.add("storm");
    } else if (index == 5) {
      changeTemp(temp, -4);
      console.log("snow")
      document.querySelector('#mountains').classList.remove("moon");
      document.querySelector('#mountains').classList.remove("storm");
      document.querySelector('#mountains').classList.add("snow");
    }

    let loadingBar = document.querySelector('.loading-bar');
    loadingBar.classList.add('active');

    setTimeout(() => {
      loadingBar.classList.remove('active');
    }, 1200);
  });
};
//............................................................... Script ...................................................................
// Data for the sections
let h1Texts = ["Pear", "Apple", "Exotic"]; // Add your h1 texts here
let logoColors = [
  "var(--pear-logo)",
  "var(--apple-logo)",
  "var(--exotic-logo)"
]; // Add your logo colors here
let keyframes = ["wave-pear-effect", "wave-apple-effect", "wave-exotic-effect"]; // Add your keyframes here
// Normal GSAP animation.......
gsap.from(".fruit-image ", { y: "-100vh", delay: 0.5 });
gsap.to(".fruit-image img", {
  x: "random(-20, 20)",
  y: "random(-20, 20)",
  zIndex: 22,
  duration: 2,
  ease: "none",
  yoyo: true,
  repeat: -1
});

// get the elements
const waveEffect = document.querySelector(".wave");
const sections = document.querySelectorAll(".section");
const prevButton = document.getElementById("prevButton");
const nextButton = document.getElementById("nextButton");
const caneLabels = document.querySelector(".cane-labels");
const sectionContainer = document.querySelector(".section-container");
// Set index and current position
let index = 0;
let currentIndex = 0;
let currentPosition = 0;

// Add event listeners to the buttons
nextButton.addEventListener("click", () => {
  // Decrease the current position by 100% (to the left)
  if (currentPosition > -200) {
    currentPosition -= 100;
    // Update the left position of the cane-labels
    caneLabels.style.left = `${currentPosition}%`;
    sectionContainer.style.left = `${currentPosition}%`;
  }
  // Increment index and currentIndex
  currentIndex++;
  // Update the h1 text if currentIndex is less than the length of h1Texts
  if (currentIndex < h1Texts.length) {
    document.querySelector(".h1").innerHTML = h1Texts[currentIndex];
  }
  // Gasp animation for next section components
  gsap.to(".logo", {
    opacity: 1,
    duration: 1,
    color: logoColors[currentIndex]
  });
  gsap.from(".h1", { y: "20%", opacity: 0, duration: 0.5 });
  gsap.from(".fruit-image ", { y: "-100vh", delay: 0.4, duration: 0.4 });

  // Disable the nextButton if the last section is active
  if (currentIndex === h1Texts.length - 1) {
    nextButton.style.display = "none";
  }
  // Enable the prevButton if it's not the first section
  if (currentIndex > 0) {
    prevButton.style.display = "block";
  }
  // Button colors and animations
  nextButton.style.color = logoColors[currentIndex + 1];
  prevButton.style.color = logoColors[currentIndex - 1];
  nextButton.style.animationName = keyframes[currentIndex + 1];
  prevButton.style.animationName = keyframes[currentIndex - 1];
});
// Add event listeners to the buttons
prevButton.addEventListener("click", () => {
  if (currentPosition < 0) {
    currentPosition += 100;
    // Update the left position of the cane-labels
    caneLabels.style.left = `${currentPosition}%`;
    sectionContainer.style.left = `${currentPosition}%`;
    sectionContainer.style.transition = `all 0.5s ease-in-out`;
  }
  // Decrement index and currentIndex
  currentIndex--;
  if (currentIndex >= 0) {
    document.querySelector(".h1").innerHTML = h1Texts[currentIndex];
  }
  // Gasp animation for previous section components
  gsap.to(".logo", { color: logoColors[currentIndex], duration: 1 });
  gsap.from(".h1", { y: "20%", opacity: 0, duration: 0.5 });
  gsap.from(".fruit-image ", { y: "100vh", delay: 0.5 });
  // Enable the nextButton if it was disabled
  nextButton.style.display = "block";
  // Disable the prevButton if it's the first section
  if (currentIndex === 0) {
    prevButton.style.display = "none";
  }
  // Button colors and animations
  nextButton.style.color = logoColors[currentIndex + 1];
  prevButton.style.color = logoColors[currentIndex - 1];
  nextButton.style.animationName = keyframes[currentIndex + 1];
  prevButton.style.animationName = keyframes[currentIndex - 1];
});