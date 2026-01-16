/*

*/

  var APIServer = document.URL;
  var LoggID = 0;
  var LoggTypeID = 0;
  var LoggTittel = "";
  var LoggBeskrivelse = "";
  var LoggKallesignal = "";
  var LogglinjeTidsstempel = 0;
  var LagslisteTidsstempel = 0;
  var TimerLogglisteID = 0;
  var TimerLagslisteID = 0;
  var OpprettLagAutomatisk = 1;

  function OppdaterKlokke() {
    var ManedNavn = ["jan", "feb", "mar", "apr", "mai", "jun", "jul", "aug", "sep", "okt", "nov", "des"];
    var DagNavn = ["S&#248;ndag", "Mandag", "Tirsdag", "Onsdag", "Torsdag", "Fredag", "L&#248;rdag"];

    var currentTime = new Date();
    var currentHours = currentTime.getHours();
    var currentMinutes = currentTime.getMinutes();
    var currentSeconds = currentTime.getSeconds();

    tidTime = (currentHours < 10 ? "0" : "") + currentHours;
    tidMinutter = ( currentMinutes < 10 ? "0" : "" ) + currentMinutes;
    tidSekunder = ( currentSeconds < 10 ? "0" : "" ) + currentSeconds;

    $("#KlokkeTid").html(tidTime+":"+tidMinutter+":"+tidSekunder);
    $("#KlokkeDato").html(DagNavn[currentTime.getDay()] + " " + currentTime.getDate() + " " + ManedNavn[currentTime.getMonth()] + " " + currentTime.getFullYear());
    $("#SkjemaDTG").val(currentTime.getDate()+tidTime+tidMinutter);
  }

  function ResetVindu() {
    LoggID = 0;
    LoggTypeID = 0;
    LoggTittel = "";
    LoggBeskrivelse = "";
    LoggKallesignal = "";
    LogglinjeTidsstempel = 0;
    LagslisteTidsstempel = 0;
    document.title = "SARLog3";
    ResetListeTopp();
    ResetLoggliste();
    clearInterval(TimerLogglisteID);
    clearInterval(TimerLagslisteID);
    $("#SideLagsliste").hide();
    $("#SideLagsliste").empty();
    $("#Bunnskjema").hide();
    $("#Bunnskjema").empty();
  }

  function ResetListeTopp() {
    $("#ListeTopp").empty();
  }

  function ResetLoggliste() {
    $("#ListeLogglinjer").empty();
    content = "  <tr class=\"Normal\">";
    content += "    <td colspan=\"5\" class=\"KolonneMelding\"><center><i>---- Ingen logglinjer å vise ----</i></center></td>";
    content += "  </tr>";
    $(content).appendTo("#ListeLogglinjer");
  }

  function ResetSkjema() {
    if (LoggTypeID == 0) {
      $("#SkjemaDTG").val("");
      $("#SkjemaMelding").val("");
      $("#SkjemaMelding").focus();
    } else if (LoggTypeID == 1) {
      $("#MeldingSlutt").val("0");
      $("#SkjemaFra").css("background-color","#fff");
      $("#SkjemaTil").css("background-color","#fff");
      $("#SkjemaDTG").val("");
      $("#SkjemaFra").val("");
      $("#SkjemaTil").val("");
      $("#SkjemaMelding").val("");
      $("#SkjemaTil").focus();
    } else if (LoggTypeID == 2) {
      $("#SkjemaDTG").val("");
      $("#SkjemaMelding").val("");
      $("#SkjemaMelding").focus();
    }
  }

  function ResetSkjemaOver() {
    $("#MeldingSlutt").val("0");
    $("#SkjemaFra").css("background-color","#fff");
    $("#SkjemaTil").css("background-color","#fff");
    $("#SkjemaDTG").val("");
    var temp = $("#SkjemaFra").val();
    $("#SkjemaFra").val($("#SkjemaTil").val());
    $("#SkjemaTil").val(temp);
    $("#SkjemaMelding").val("");
    $("#SkjemaMelding").focus();
  }

  function HentLoggInfo() {
    if (LoggID != 0) {
      $.getJSON(APIServer+"api.php/logg/logg_data?id="+LoggID+"&uts="+new Date().getTime(), function(data) {
        if (data != null) {
          LoggTypeID = data.TypeID;
          LoggTittel = data.Tittel;
          LoggBeskrivelse = data.Beskrivelse;
          LoggKallesignal = data.Kallesignal;
          document.title = "SARLog: "+data.Tittel;
          OppsettListeTopp();
          OppdaterLoggliste();
          TimerLogglisteID = setInterval("OppdaterLoggliste()", 20000);
          OppsettBunnskjema();
          $("#Bunnskjema").show();
        }
      });
    }
  }

  function OppsettListeTopp() {
    $("#ListeTopp").empty();
    content = "";
    if (LoggTypeID == 0) {
      content += "<span class=\"KolonneNr\">NR</span>";
      content += "<span class=\"KolonneDTG\">DTG</span>";
      content += "<span class=\"KolonneMelding\">MELDING</span>";
    } else if (LoggTypeID == 1) {
      content += "<span class=\"KolonneNr\">NR</span>";
      content += "<span class=\"KolonneDTG\">DTG</span>";
      content += "<span class=\"KolonneTil\">TIL</span>";
      content += "<span class=\"KolonneFra\">FRA</span>";
      content += "<span class=\"KolonneMelding\">MELDING</span>";
    } else if (LoggTypeID == 2) {
      content += "<span class=\"KolonneNr\">NR</span>";
      content += "<span class=\"KolonneDTG\">DTG</span>";
      content += "<span class=\"KolonneMelding\">SKADE OG BEHANDLING</span>";
    }
    $(content).appendTo("#ListeTopp");
  }


  function OppsettBunnskjema() {
    $("#Bunnskjema").empty();
    content = "";
    if (LoggTypeID == 0) {
      content += "<form action=\"index.php\" method=\"POST\" id=\"SkjemaAL\">";
      content += "<input type=\"hidden\" name=\"SkjemaDTG\" id=\"SkjemaDTG\" />";
      content += "<div id=\"DIVSkjemaMelding0\"><textarea name=\"SkjemaMelding\" id=\"SkjemaMelding\" placeholder=\"Melding\" style=\"width:98%;height:98%;\"></textarea></div>";
      content += "<div id=\"DIVSkjemaKnapp\"><button id=\"KnappLagreSkjema\" style=\"display:block;height:78px;width:100px;\" accesskey=\"L\"><u>L</u>agre</button></div>";
      content += "</form>";
      $(content).appendTo("#Bunnskjema");
      OppsettSkjemaAL();
    } else if (LoggTypeID == 1) {
      content += "<form action=\"index.php\" method=\"POST\" id=\"SkjemaSL\">";
      content += "<input type=\"hidden\" name=\"MeldingSlutt\" id=\"MeldingSlutt\" />";
      content += "<input type=\"hidden\" name=\"SkjemaDTG\" id=\"SkjemaDTG\" />";
      content += "<div id=\"DIVSkjemaTil\"><input type=\"text\" name=\"SkjemaTil\" id=\"SkjemaTil\" size=\"6\" maxlength=\"3\" placeholder=\"Til xx\" style=\"width:98%\" /></div>";
      content += "<div id=\"DIVSkjemaFra\"><input type=\"text\" name=\"SkjemaFra\" id=\"SkjemaFra\" size=\"6\" maxlength=\"3\" placeholder=\"Fra xx\" style=\"width:98%\" /></div>";
      content += "<div id=\"DIVSkjemaMelding1\"><textarea name=\"SkjemaMelding\" id=\"SkjemaMelding\" placeholder=\"Melding\" style=\"height:98%;width:98%;\"></textarea></div>";
      content += "<div id=\"DIVSkjemaKnapp\"><button id=\"KnappLagreSkjemaOver\" style=\"display:block;height:35px;width:100px;\" accesskey=\"O\"><u>O</u>ver</button><br /><button id=\"KnappLagreSkjemaSlutt\" style=\"display:block;height:35px;width:100px\" accesskey=\"S\"><u>S</u>lutt</button></div>";
      content += "</form>";
      $(content).appendTo("#Bunnskjema");
      OppsettSkjemaSL();
    } else if (LoggTypeID == 2) {
      content += "<form action=\"index.php\" method=\"POST\" id=\"SkjemaSAN\">";
      content += "<input type=\"hidden\" name=\"SkjemaDTG\" id=\"SkjemaDTG\" />";
      content += "<div id=\"DIVSkjemaMelding0\"><textarea name=\"SkjemaMelding\" id=\"SkjemaMelding\" placeholder=\"Melding\" style=\"height:98%;width:98%;\"></textarea></div>";
      content += "<div id=\"DIVSkjemaKnapp\"><button id=\"KnappLagreSkjema\" style=\"display: block; height: 78px;width:100px;\" accesskey=\"L\"><u>L</u>agre</button></div>";
      content += "</form>";
      $(content).appendTo("#Bunnskjema");
      OppsettSkjemaSAN();
    }
  }
  function OppsettSkjemaAL() {
    $("#SkjemaAL").submit(function(event) {
      event.preventDefault();

      var FormStatus = 1;

      if ($("#SkjemaMelding").val().length == 0) {
        alert("Logglinjen må inneholde en melding!");
        FormStatus = 0;
      }

      if (FormStatus == 1) {
        var NyLinje = new Object();
        NyLinje.DTG = $("#SkjemaDTG").val();
        NyLinje.Melding = $("#SkjemaMelding").val();
        var jqxhr = $.post(APIServer+"api.php/logg/lagrelinje?loggid="+LoggID, NyLinje )
          .success(function() {
            ResetSkjema();
            OppdaterLoggliste();
          })
          .error(function(data) {
            alert("En feil oppstod!"+data.responseText);
          })
        delete NyLinje;
      }
    });
  }

  function OppsettSkjemaSL() {
    $("#SkjemaSL").submit(function(event) {
      event.preventDefault();

      var FormStatus = 1;

      if (($("#SkjemaFra").val().length == 0) && ($("#SkjemaTil").val().length == 0)) {
        alert("Feltene til og fra kan ikke begge v&#230;re tomme!");
        FormStatus = 0;
      } else {
        if ($("#SkjemaFra").val().length == 0) {
          $("#SkjemaFra").val(Kallesignal);
        }
        if ($("#SkjemaTil").val().length == 0) {
          $("#SkjemaTil").val(Kallesignal);
        }
      }
      if ($("#SkjemaMelding").val().length == 0) {
        alert("Logglinjen m&#229; inneholde en melding!");
        FormStatus = 0;
      }

      if (FormStatus == 1) {
        var NyLinje = new Object();
        NyLinje.OpprettLagAutomatisk = OpprettLagAutomatisk;
        NyLinje.DTG = $("#SkjemaDTG").val();
        NyLinje.Fra = $("#SkjemaFra").val();
        NyLinje.Til = $("#SkjemaTil").val();
        NyLinje.Melding = $("#SkjemaMelding").val();
        NyLinje.Slutt = $("#MeldingSlutt").val();
        var jqxhr = $.post(APIServer+"api.php/logg/lagrelinje?loggid="+LoggID, NyLinje )
          .success(function() {
            if ($("#MeldingSlutt").val() == 1) {
              ResetSkjema();
            } else {
              ResetSkjemaOver();
            }
            OppdaterLoggliste();
            OppdaterLagsliste();
          })
          .error(function(data) {
            alert("En feil oppstod!"+data.responseText);
          })
        delete NyLinje;
      }
    });
    $("#KnappLagreSkjemaOver").click(function() {
      $("#MeldingSlutt").val("0");
    });
    $("#KnappLagreSkjemaSlutt").click(function() {
      $("#MeldingSlutt").val("1");
    });
  }

  function OppsettSkjemaSAN() {
    $("#SkjemaSAN").submit(function(event) {
      event.preventDefault();

      var FormStatus = 1;

      if ($("#SkjemaMelding").val().length == 0) {
        alert("Logglinjen må inneholde en melding!");
        FormStatus = 0;
      }

      if (FormStatus == 1) {
        var NyLinje = new Object();
        NyLinje.DTG = $("#SkjemaDTG").val();
        NyLinje.Melding = $("#SkjemaMelding").val();
        var jqxhr = $.post(APIServer+"api.php/logg/lagrelinje?loggid="+LoggID, NyLinje )
          .success(function() {
            ResetSkjema();
            OppdaterLoggliste();
          })
          .error(function(data) {
            alert("En feil oppstod!"+data.responseText);
          })
        delete NyLinje;
      }
    });
  }

  function OppdaterLoggliste() {
    if (LoggID != 0) {
      $.getJSON(APIServer+"api.php/logg/linjer?loggid="+LoggID+"&uts=" + new Date().getTime(), function(data) {
        if (data.Linjer != null) {
          //if (data.DatoSisteMelding > LogglinjeTidsstempel) {
            $("#ListeLogglinjer").empty();

            $.each(data.Linjer, function(i,linje) {
              content = "";
              if (linje.Nr % 2 == 0) {
                content += "  <tr class=\"Odd\">";
              } else {
                content += "  <tr>";
              }
              content += "    <td class=\"KolonneNr\">" + linje.Nr + "</td>";
              content += "    <td class=\"KolonneDTG\">" + linje.DTG + "</td>";
              if (LoggTypeID == 1) {
                if (linje.Til == LoggKallesignal) {
                  content += "    <td class=\"KolonneTil\">" + linje.Til + "</td>";
                } else {
                  content += "    <td class=\"KolonneTil\"><b>" + linje.Til + "</b></td>";
                }
                if (linje.Fra == LoggKallesignal) {
                  content += "    <td class=\"KolonneFra\">" + linje.Fra + "</td>";
                } else {
                  content += "    <td class=\"KolonneFra\"><b>" + linje.Fra + "</b></td>";
                }
              }
              content += "    <td class=\"KolonneMelding\" >" + linje.Melding + "</td>";
              content += "  </tr>";
              $(content).appendTo("#ListeLogglinjer");
              $('#Sideinnhold').scrollTop($('#Sideinnhold')[0].scrollHeight);
            });
            LogglinjeTidsstempel = data.DatoSisteMelding;
            $(".LogglinjeNy").effect("pulsate");
          //}
        } else {
          if (data.Linjer == 0) {
            ResetLoggliste();
          }
        }
      });
    }
  }

  /*function OppdaterLagsliste() {
    if (AktivitetID > 0) {
      $.getJSON(APIServer+"api.php/mannskap/lagsliste?aktivitetid="+AktivitetID+"&uts=" + new Date().getTime(), function(data) {
      if (data.Lagsliste != null) {
        if (data.SistEndret > LagslisteTidsstempel) {
          $("#SideLagsliste").empty();
          content = "";

          $.each(data.Lagsliste, function(i, lag) {
            if (lag.Kallesignal.length > 0) {
              content += "<div class=\"LagsInfo\">";
              content += "<table class=\"LagstatusUkjent\">";
              content += "  <tr>";
              content += "    <td class=\"Kallesignal\">"+lag.Kallesignal+"</td>";
              content += "    <td class=\"Lagsnavn\">"+lag.Navn+"</td>";
              if (lag.SisteMeldingMinutter > 30) {
                content += "    <td class=\"Tid\"><img src=\"/grafikk/clock_icon.png\" height=\"10\">&nbsp;"+lag.SisteMeldingMinutter+"</td>";
              } else {
                content += "    <td class=\"Tid\">&nbsp;</td>";
              }
              content += "  </tr>";
              content += "</table>";
              content += "</div>";
            }
          });

          $(content).appendTo("#SideLagsliste");
          LagslisteTidsstempel = data.SistEndret;
        }
      }
    });
    }
  }*/

  function LoggApne(NyLoggID) {
    ResetVindu();
    LoggID = NyLoggID;
    HentLoggInfo();
  }

  $(document).ready(function() {
    OppdaterKlokke();
    setInterval("OppdaterKlokke()", 1000);
    ResetVindu();
  });