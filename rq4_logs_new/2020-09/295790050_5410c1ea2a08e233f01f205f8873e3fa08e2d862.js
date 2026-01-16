function load(){
	var array = document.getElementsByName("region");
      array.forEach(function(item, i, array)
      {
      	var attr = GetFractionName(item.getAttribute('data'));
        if (attr.name == "Орда") item.style.fill = '#8B0000';
        if (attr.name == "Союз племён") item.style.fill = '#808000';
        if (attr.name == "Аэдирн и Каэдвен") item.style.fill = '#FFFF00';
        if (attr.name == "Данмерский Доминион") item.style.fill = '#4B0082';
        if (attr.name == "Редания и Темерия") item.style.fill = '#FF0000';

      });
}
	function MakeClicker() {
    var classname = document.getElementsByClassName("region");
    load();
    for (var i = 0; i < classname.length; i++) {
    classname[i].addEventListener('click', myFunction, false);
    }
    };
var myFunction = function() {
	var prov = provinces[this.getAttribute("data")];
      var attr = GetFractionName(this.getAttribute("data"));
      var maker = document.getElementById("fractionName");
      maker.innerHTML = "Название фракции: " + attr.name;
      MakeIdeology(this.getAttribute("data"));
      MakeReligion(this.getAttribute("data"));
      maker = document.getElementById("fractionResourses");
      maker.innerHTML = "Сила армии: " + attr.army + ", Благосостояние " + attr.wealth;
      MakeAgents(this.getAttribute("data"));
      maker = document.getElementById("provinceName");
      maker.innerHTML = "Название провинции: " + prov.name;
      maker = document.getElementById("provinceDefence");
      maker.innerHTML = "Уровень укрепления: " + prov.defence;
      maker = document.getElementById("provinceCapitol");
      if (prov.capitol == "да") maker.innerHTML = "Cтолица государства";
      else maker.innerHTML = "";
      maker = document.getElementById("provinceTreasury");
      maker.innerHTML = "Сокровищница: " + prov.treasury;
      maker = document.getElementById("provinceCasern");
      maker.innerHTML = "Казарма: " + prov.casern;
      maker = document.getElementById("provinceChapel");
      maker.innerHTML = "Часовня: " + prov.chapel;
      };
function GetFractionName(idGet)
{
	var idFr = provinces[idGet].owner;
	return fractions[idFr];
};
function MakeIdeology(idGet)
{
  var frac = GetFractionName(idGet);
  var ideolog = ideologies[frac.ideology];
  var maker = document.getElementById("fractionIdeology");
  maker.innerHTML = "Идеология: " + ideolog.name;
  maker = document.getElementById("ideologyStat");
  maker.innerHTML = "Поддержка: " + frac.nationalizm + "Н, " + frac.kommunizm + "К, " + frac.monarchy + "М, " + frac.democracy + "А";
}
function MakeReligion(idGet)
{
  var frac = GetFractionName(idGet);
  var relig = religions[frac.religion];
  var maker = document.getElementById("fractionReligion");
  maker.innerHTML = "Религия: " + relig.name;
  maker = document.getElementById("religionStat");
  maker.innerHTML = "Поддержка: " + frac.fanatizm + "Ф, " + frac.pacifizm + "П, " + frac.pangolizm + "Я, " + frac.ateizm + "А";
}
function MakeAgents(idGet)
{
	var frac = GetFractionName(idGet);
	var agent = agents[frac.warchief];
  MakeAgent(agent, "agentWar", "warStat");
  agent = agents[frac.diplomat];
  MakeAgent(agent, "agentDip", "dipStat");
	agent = agents[frac.spy];
  MakeAgent(agent, "agentSpy", "spyStat");
	agent = agents[frac.stewart];
  MakeAgent(agent, "agentSte", "steStat");
	agent = agents[frac.confessor];
  MakeAgent(agent, "agentConf", "confStat");
};
function MakeAgent(agent, idLabel, idStat)
{
  var maker = document.getElementById(idLabel);
  maker.innerHTML = agent.type + " : " + agent.name;
  maker = document.getElementById(idStat);
  maker.innerHTML = "Авторитет " + agent.honor + " , Рвение " + agent.vigor + " , Хитрость " + agent.cunning;
}



    