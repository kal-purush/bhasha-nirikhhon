function onLoad(f)
    {
        if (onLoad.loaded) // Если документ уже загрузился
            window.setTimeout(f, 0); //Запустим f как только сможем
        window.addEventListener("load", f, false); //если документ еще не загрузился
    }

var generateTOC = onLoad(function()
    {
        var toc = document.getElementById("TOC");
        if (!toc)
        {
            toc = document.createElement("div");
            toc.id = "TOC";
            document.body.insertBefore(toc, document.body.firstChild);
        }
        // Собираем все заголвки в документе
        var headings;
        headings = document.querySelectorAll("h1,h2,h3,h4,h5,h6");

        // Массив, хранящий текущие номера разделов
        var sectionNumbers = [0,0,0,0,0,0];

        // Идем по собранным заголовкам
        for(var h = 0; h < headings.length; h++)
        {
            var heading = headings[h];
            //далее работаем с заголовком в документе: heading

            if (heading.parentNode == toc) continue; // пропускаем само оглавление
            //второй символ тэга <h*> точно скажет нам, какой у нас уровень
            var level = parseInt(heading.tagName.charAt(1));
            if (isNaN(level) || level < 1 || level > 6) continue;

            //Для текущего уровня списка поднимем номер, а следующие скинем на 0
            sectionNumbers[level-1]++;
            for(var i = level; i < 6; i++) sectionNumbers[i] = 0;

            //Соберем текущий номер раздела из массива
            var sectionNumber = sectionNumbers.slice(0,level).join(".");

            //Для стилизации занесем номер в <span>
            var span = document.createElement("span");
            span.className = "TOCSectNum";
            span.innerHTML = sectionNumber;
            //Присоединим номер к заголовку раздела
            heading.insertBefore(span, heading.firstChild);

            //"обернем" заголовок раздела в якорь со ссылкой вида "#TOC..."
            var anchor = document.createElement("a");
            anchor.id = "TOC"+sectionNumber;
            //Вставим <a> перед заголовком
            heading.parentNode.insertBefore(anchor, heading);
            //А затем перекинем заголовок внутрь <a>
            anchor.appendChild(heading);

            //А это мы уже создаем ссылку в содержании
            var link = document.createElement("a");
            link.href = "#TOC" + sectionNumber; //адрес созданного якоря в документе
            link.innerHTML = heading.innerHTML; //текст заголовка - в содержание

            // создадим для заголовка раздела отдельный <div> в содержании
            var entry = document.createElement("div");
            entry.className = "TOCEntry TOCLevel" + level;
            entry.appendChild(link);

            //Прицепим новый заголовок содержания со ссылкой к содержанию.
            toc.appendChild(entry);
        }
    }
);