$(document).ready(() => {
    const kategorija = $('#select');
    const kod = $('#barkod');
    const naziv = $('#nazivArtikla');
    const opis = $('#opisArtikla');
    const vrsta = $('#vrstaArtikla');
    const cena1 = $('#osnovnaCena');
    const cena2 = $('#cenaPdv');
    const dugme = $('#dugme');



    kod.keyup(function (e) {
        if (/\D/g.test(this.value)) {
            this.value = this.value.replace(/\D/g, '');
        }
    });

    kod.change(function () {
        if (kod.val() !== '') {
            kod.removeClass('redBorder');
        } else {
            kod.addClass('redBorder');
        }
        submitFunction();
    });

    naziv.change(function () {
        if (naziv.val() !== '') {
            naziv.removeClass('redBorder');
        } else {
            naziv.addClass('redBorder');
        }
        submitFunction();
    });

    vrsta.change(function(){
        if (vrsta.val() !== '') {
            vrsta.removeClass('redBorder');
        } else {
            vrsta.addClass('redBorder');
        }
        submitFunction();
    });

    cena1.keyup(function (e) {
        if (/\D/g.test(this.value)) {
            this.value = this.value.replace(/\D/g, '');
        }
    });

    
    let d;
    let vreme;

    cena1.change(function () {
        cena2.val(cena1.val() * 1.2);
         d = new Date($.now());
         vreme = d.getDate()+"-"+(d.getMonth() + 1)+"-"+d.getFullYear()+" "+d.getHours()+":"+d.getMinutes()+":"+d.getSeconds();

    });

 


    function submitFunction() {
        if (kod.val() == '' || naziv.val() == '' || vrsta.val() == '') {
            dugme.attr("disabled", true);
        } else {
            dugme.removeAttr("disabled");
        }
    }

    
    dugme.click(function (e) {
        e.preventDefault(e);
        $('table').css('visibility', 'visible');
        const red = '<tr><td>'+ kategorija.val() +'</td><td>'+ kod.val() +'</td><td>'
                    + naziv.val() + '</td><td id="limit" title="'+opis.val()+'">'+ opis.val() +'</td><td>'+ vrsta.val()
                     +'</td><td>'+ cena1.val() +'</td><td>'+cena2.val() +'</td><td>'+ vreme +'</td></tr>';
        $('tbody').append(red);
        kod.addClass('redBorder');
        naziv.addClass('redBorder');
        vrsta.addClass('redBorder');
        dugme.attr("disabled", true);



        $("#table tr").css("background-color", function(index) {
            return index%2==0?"yellow":"orange";
        });

        kod.val('');
        naziv.val('');
        opis.val('');
        vrsta.val('');
        cena1.val('');
        cena2.val('');
    });

    $('.search').keyup(function(){
        let searchTerm = $('.search').val();
        let listItem = $('result tbody').children('tr');
        let searchSplit = searchTerm.replace(/ /g,"'):containsi('");
        $.extend($.expr[':'], {
            'containsi': function(elem, i, match, array){
                return (elem.textContent || elem.innerText || '').toLoverCase().indexOf((match[2] || "").toLowerCase()) >=0;
            }
        });
        $(".results tbody tr").not(":containsi('"+ searchSplit +"')").each(function(){
            $(this).attr('visible', 'false');
        })
        $(".results tbody tr:containsi('"+ searchSplit +"')").each(function(){
            $(this).attr('visible', 'true');
        })
        let jobCount = $('.results tbody tr[visible="true"]').length;
        $('.counter').text(jobCount +'item');
        if(jobCount === 0){
            $('no-result').show();
        }


    });








});


