var neo4j = require('neo4j-driver').v1;

var simpleProps = ["entity_name", "entity_type", "edrpou", "certificate_series",
					"certificate_number", "certified_date", "address", "phone", "email", "service_type"];
var regexProps = ["entity_name", "entity_type", "address", "service_type", "manager_full_name"];

var labels = ["Повне найменування", "Тип установи", "Код за ЄДРПОУ", "Серія сертифіката",
				"Номер сертифіката", "Адреса", "Телефон", "Адреса електронної пошти",
				"Вид послуги"];

var stupidObjToStr = function(obj1) {
	var result = "{";
	for (var key in obj1) {
		var value = String(obj1[key]);
		if (value == "[object Object]")
			value = stupidObjToStr(obj1[key]);
		else if (typeof(obj1[key]) == 'string') {
			var parts = value.split("\'");
			var repr = "";
			for (var i = 0; i < parts.length; i++)
				repr += parts[i] + "\\\'";
			repr = repr.substr(0, repr.length - 2);
			value = "\'" + repr + "\'";
		}
		result += key + ": " + value + ", ";
	}
	if (result.length == 1)
		return "{}";
	else
		return result.substr(0, result.length - 2) + "}";
};

var standartErrorSend = function(error) {
	res.json({error: true, message: stupidObjToStr(error)});
}

var getAnswerDictStr = function(entVarName, indVarName) {
	var returnString = "{";
	for (var i = 0; i < simpleProps.length; i++)
		returnString += simpleProps[i] + ": " + entVarName + "." + simpleProps[i] + ", ";
	returnString = returnString + "_id: ID(" + entVarName + "), manager_full_name: " + indVarName + ".full_name}";
	return returnString;
};

var parseDate = function(str) {
	var g1 = str.split('.');
	if (g1.length != 3)
		return null;
	for (var i = 0; i < 3; i++)
		if (isNaN(g1[i] * 0))
			return null;
	var mbResult = new Date(g1[2], g1[1] - 1, g1[0]);
	if ((mbResult.getDate() != g1[0]) || (mbResult.getMonth() != g1[1] - 1) || (mbResult.getFullYear() != g1[2]))
		return null;
	return str;
};

var tokenizeSecondPart = function(secondPart) {
	if (secondPart.length == 0) {
		return {};
	}
	else if (secondPart.indexOf('=') == -1) {
		return {"_id": secondPart};
	}
	else if (secondPart.indexOf('&') == -1) {
		var i1 = secondPart.indexOf("=") + 1;
		var res = {};
		res[secondPart.substring(0, i1 - 1)] = secondPart.substring(i1);
		return res;
	}
	else {
		var i1 = secondPart.indexOf("&");
		if (secondPart.substring(i1 + 1)[0] == "&") {
			throw new Error("");
		}
		var left = tokenizeSecondPart(secondPart.substring(0, i1));
		var right = tokenizeSecondPart(secondPart.substring(i1 + 1));
		for (var key1 in right) {
			left[key1] = right[key1];
		}
		return left;
	}
};

module.exports.getLegalEntity = function(req, res, session) {
	var filter = tokenizeSecondPart(req.params.filter1);
	if (filter.certified_date != null){
		var d1 = parseDate(value);
		if (d1 == null) {
			res.json({"message": "Неправильний формат дати. Правильний: ДД.ММ.РРРР", error: true});
			return;
		}
		filter.certified_date = d1;
	}
	if (filter.edrpou != null)
		filter.edrpou = Number(filter.edrpou);
	var id = null;
	if (filter._id != null) {
		id = filter._id;
		delete filter._id;
	}
	var whereNeeded = (id != null);
	var presentRegexProps = {};
	for (var i = 0; i < regexProps.length; i++)
		if (regexProps[i] in filter) {
			presentRegexProps[regexProps[i]] = filter[regexProps[i]];
			whereNeeded = true;
			delete filter[regexProps[i]];
		}
	var filterStr = stupidObjToStr(filter);
	var dictStr1 = getAnswerDictStr("en1", "manager");
	var dictStr2 = getAnswerDictStr("aff1", "aff_manager");
	var query1String = "MATCH (manager:Individual)-[:MANAGER]->(en1:LegalEntity " + filterStr + ")";
	query1String += (whereNeeded ? " WHERE " : "");
	var trimEnd = false;
	for (var key1 in presentRegexProps) {
		query1String += ((key1 == 'manager_full_name') ? "manager.full_name" : ("en1." + key1)) + " =~ \'(?i).*" + presentRegexProps[key1] + ".*\' AND ";
		trimEnd = true;
	}
	if (id != null) {
		query1String += "ID(en1) = " + id;
		trimEnd = false;
	}
	if (trimEnd)
		query1String = query1String.substr(0, query1String.length - 5);
	query1String += " OPTIONAL MATCH (en1)-[:POSESSES]->(license:License)";
	query1String += " RETURN " + dictStr1 + " AS data, CASE license IS NULL WHEN true THEN [] \
ELSE collect({start_date: license.start_date, due_date: license.due_date, service_name: license.service_name, \
license_number: license.license_number}) END AS licenses;";
	var result = [];
	console.log(query1String + "\n");
	session.run(query1String).subscribe({
		onNext: function(record) {
			var _id = record.get("data")._id.toNumber();
			var edrpou = record.get("data").edrpou.toNumber();
			var toPush = record.get("data");
			toPush._id = _id;
			toPush.edrpou = edrpou;
			toPush.licenses = record.get("licenses");
			toPush.affiliates = [];
			result.push(toPush);
		},
		onCompleted: function() {
			var ids = [];
			for (var i = 0; i < result.length; i++)
				ids.push(result[i]._id);
			var query2String = "UNWIND {ids} as id1 \
MATCH (en1:LegalEntity)-[:PARENT]->(aff1:LegalEntity)<-[:MANAGER]-(aff_manager:Individual) \
WHERE ID(en1) = id1 RETURN collect(" + dictStr2 + ") AS affiliates, id1";
			console.log(query2String + "\n");
			session.run(query2String, {"ids": ids}).subscribe({
				onNext: function(record) {
					//protection from magic Level 1
					var affiliates = record.get("affiliates");
					for (var i = 0; i < affiliates.length; i++) {
						affiliates[i]._id = affiliates[i]._id.toNumber();
						affiliates[i].edrpou = affiliates[i].edrpou.toNumber();
					}
					var id1 = record.get("id1");
					result[ids.indexOf(id1)].affiliates = affiliates;
				},
				onCompleted: function() {
					res.json(result);
				},
				onError: standartErrorSend
			});
		},
		onError: standartErrorSend
	});
};

module.exports.postLegalEntity = function(req, res, session) {
	if (req.body.certified_date) {
		var d1 = parseDate(req.body.certified_date);
		if (d1 == null) {
			res.json({"message": "Неправильний формат дати. Правильний: ДД.ММ.РРРР", "error": true});
			return;
		}
		else
			req.body.certified_date = d1;
	}
	else {
		var k1 = new Date();
		req.body.certified_date = k1.getDate() + "." + (k1.getMonth() + 1) + "." + k1.getFullYear();
	}
	req.body.edrpou = Number(req.body.edrpou);
	for (var i = 0; i < simpleProps.length; i++) {
		var key1 = simpleProps[i];
		if ((req.body[key1] == null) && (key1 != 'email') && (key1 != 'phone')) {
			res.json({message: "Обов'язкове поле \"" + labels[i] + "\" не заповнене", error: true});
			return;
		}
	}
	var affiliates = [];
	for (var i = 0; i < req.body.affiliates.length; i++)
		affiliates.push(req.body.affiliates[i]);
	delete req.body.affiliates;
	var checkForExistQuery = "MATCH (a:LegalEntity) WHERE a.entity_name = {entity_name} OR a.edrpou = {edrpou} \
WITH collect(a.edrpou) AS found RETURN CASE (length(found) <> 0) \
WHEN false THEN {error: false} \
ELSE CASE (found[0] = {edrpou}) \
WHEN true THEN {error: true, message: {message1}} \
ELSE {error: true, message: {message2}} \
END END AS result;";
	var params = {message1: "Фінансова установа з таким кодом за ЄДРПОУ вже існує", message2: "Фінансова установа з такою назвою вже існує",
					edrpou: req.body.edrpou, entity_name: req.body.entity_name};
	var result;
	console.log(checkForExistQuery + "\n");
	session.run(checkForExistQuery, params).subscribe({
		onNext: function(record) {
			result = record.get("result");
		},
		onCompleted: function() {
			if (result.error) {
				res.json(result);
				return;
			}
			var affiliatesPresentQuery = "UNWIND {affiliates} as affiliateName \
MATCH (a:LegalEntity {entity_name: affiliateName}) RETURN collect(a.entity_name) AS presentOnes;";
			var presentOnes = [];
			console.log(affiliatesPresentQuery + "\n");
			session.run(affiliatesPresentQuery, {"affiliates": affiliates}).subscribe({
				onNext: function(record) {
					presentOnes = record.get("presentOnes");
				},
				onCompleted: function() {
					if (presentOnes.length != affiliates.length) {
						for (var i = 0; i < affiliates.length; i++)
							if (presentOnes.indexOf(affiliates[i]) == -1) {
								res.json({message: "Установа \"" + affiliates[i] + "\" не була знайдена в Реєстрі",
										error: true});
								return;
							}
					}
					var licenses = req.body.licenses;
					delete req.body.licenses;
					var manager_full_name = req.body.manager_full_name;
					delete req.body.manager_full_name;
					var dataRepr = stupidObjToStr(req.body);
					var licensesToStr = "[";
					for (var i = 0; i < licenses.length; i++) {
						if (licenses[i].due_date == null)
							licenses[i].due_date = "-";
						licensesToStr += stupidObjToStr(licenses[i]) + ", ";
					}
					if (licenses.length != 0)
						licensesToStr = licensesToStr.substr(0, licensesToStr.length - 2);
					licensesToStr += "]";

					var createQuery = "MERGE (d:Individual {full_name: {managerFullName}}) CREATE (a:LegalEntity " + dataRepr +  ") \
CREATE (d)-[:MANAGER]->(a) WITH a UNWIND " + licensesToStr + " AS license MERGE (b:License {start_date: license.start_date, due_date: license.due_date, \
service_name: license.service_name, license_number: license.license_number}) CREATE (b)<-[:POSESSES]-(a) \
WITH a UNWIND {affiliates} as affiliateName MATCH (c:LegalEntity {entity_name: affiliateName}) MERGE (a)-[:PARENT]->(c);"
					console.log(createQuery + "\n");

					console.log({managerFullName: manager_full_name, "affiliates": affiliates});
					console.log();
					session.run(createQuery, {managerFullName: manager_full_name, "affiliates": affiliates})
							.catch(standartErrorSend)
							.then(function() {
								res.json({message: "Дані внесені успішно"});
							});
				},
				onError: standartErrorSend
			})
		},
		onError: standartErrorSend
	});
};

module.exports.putByIdLegalEntity = function(req, res, session) {
	if (req.body.certified_date) {
		var d1 = parseDate(req.body.certified_date);
		if (d1 == null) {
			res.json({"message": "Неправильний формат дати. Правильний: ДД.ММ.РРРР", "error": true});
			return;
		}
		else
			req.body.certified_date = d1;
	}
	var id = req.body._id;
	delete req.body._id;
	var newAffiliates = req.body.affiliates;
	delete req.body.affiliates;
	var newLicenses = req.body.licenses;
	delete req.body.licenses;
	var newManager = req.body.manager_full_name;
	delete req.body.manager_full_name;
	var licensesToStr = "[";
	for (var i = 0; i < newLicenses.length; i++) {
		if (newLicenses[i].due_date == null)
			newLicenses[i].due_date = "-";
		licensesToStr += stupidObjToStr(newLicenses[i]) + ", ";
	}
	if (newLicenses.length != 0)
		licensesToStr = licensesToStr.substr(0, licensesToStr.length - 2);
	licensesToStr += "]";
	var checkForExistQuery = "MATCH (a:LegalEntity) WHERE ID(a) = " + id + " \
WITH collect(a.edrpou) AS found RETURN CASE (length(found) <> 0) \
WHEN true THEN {error: false} \
ELSE {error: true, message: \"Така фінансова установа ще не існує\"} \
END AS result;";
	console.log(checkForExistQuery + "\n");
	var result = {};
	session.run(checkForExistQuery).subscribe({
		onNext: function(record) {
			result = record.get("result");
		},
		onCompleted: function() {
			if (result.error) {
				res.json(result);
				return;
			}

			var affiliatesPresentQuery = "UNWIND {affiliates} as affiliateName \
MATCH (a:LegalEntity {entity_name: affiliateName}) RETURN collect(a.entity_name) AS presentOnes;";
			var presentOnes = [];
			console.log(affiliatesPresentQuery + "\n");
			session.run(affiliatesPresentQuery, {"affiliates": (newAffiliates == null) ? [] : newAffiliates}).subscribe({
				onNext: function(record) {
					presentOnes = record.get("presentOnes");
				},
				onCompleted: function() {
					if ((newAffiliates != null) && (presentOnes.length != newAffiliates.length)) {
						for (var i = 0; i < newAffiliates.length; i++)
							if (presentOnes.indexOf(newAffiliates[i]) == -1) {
								res.json({message: "Установа \"" + newAffiliates[i] + "\" не була знайдена в Реєстрі",
										error: true});
								return;
							}
					}
					var updateStr1 = (simpleProps.length == 0) ? "" : " SET ";
					for (var i = 0; i < simpleProps.length; i++)
						if (simpleProps[i] in req.body) {
							var kludgeStr = stupidObjToStr({p: req.body[simpleProps[i]]});
							var valueRepr = kludgeStr.substr(4, kludgeStr.length - 5);
							updateStr1 += "a." + simpleProps[i] + " = " + valueRepr + ", ";
						}
					if (updateStr1.length != 0)
						updateStr1 = updateStr1.substr(0, updateStr1.length - 2);
					var params = {};
					var queries = [];

					if (newAffiliates != null) {
						var updateQuery3 = "MATCH (a:LegalEntity)-[c2:PARENT]->(aff1:LegalEntity) WHERE ID(a) = " + id + " DETACH DELETE aff1;";
						var updateQuery31 = " MATCH (a:LegalEntity) WHERE ID(a) = " + id + " UNWIND {newAffiliates} AS aff_name \
MATCH (aff2:LegalEntity {entity_name: aff_name}) MERGE (a)-[:PARENT]->(aff2);";
						params.newAffiliates = newAffiliates;
						queries.push(updateQuery31);
						queries.push(updateQuery3);
					}

					if (newLicenses != null) {
						var updateQuery2 = "MATCH (a:LegalEntity)-[c1:POSESSES]->(lic1:License) WHERE ID(a) = " + id + " DETACH DELETE lic1;"
						var updateQuery21 =  " MATCH (a:LegalEntity) WHERE ID(a) = " + id + " UNWIND " + licensesToStr + " AS license \
MERGE (lic2:License {start_date: license.start_date, due_date: license.due_date, service_name: license.service_name, \
license_number: license.license_number}) MERGE (a)-[:POSESSES]->(lic2);";
						queries.push(updateQuery21);
						queries.push(updateQuery2);
					}

					var updateQuery1 = "MATCH (m1:Individual)-[c0:MANAGER]->(a:LegalEntity) WHERE ID(a) = " + id + updateStr1;
					if (newManager != null) {
						updateQuery1 += " DELETE c0 MERGE (m2:Individual {full_name: {managerFullName}}) CREATE (m2)-[:MANAGER]->(a);";
						params.managerFullName = newManager;
						queries.push(updateQuery1);
					}

					var recursiveQuering = function() {
						if (queries.length == 0)
							res.json({message: "Зміни внесені успішно"});
						else {
							var query1 = queries.pop();
							console.log(query1 + "\n");
							session.run(query1, params).catch(standartErrorSend).then(recursiveQuering);
						}
					}
					recursiveQuering();
				},
				onError: standartErrorSend
			});
		},
		onError: standartErrorSend
	});
};

module.exports.deleteLegalEntity = function(req, res, session) {
	if (req.body.certified_date) {
		var d1 = parseDate(req.body.certified_date);
		if (d1 == null) {
			res.json({"message": "Неправильний формат дати. Правильний: ДД.ММ.РРРР", "error": true});
			return;
		}
		else
			req.body.certified_date = d1;
	}
	var managerFullName = req.body.manager_full_name;
	delete req.body.manager_full_name;
	var id = req.body._id;
	delete req.body._id;
	var filterStr = stupidObjToStr(req.body);
	var checkForExistQuery = "MATCH (manager:Individual " + ((managerFullName == null) ? "" : stupidObjToStr({full_name: managerFullName})) +
	")-[c1:MANAGER]->(en1:LegalEntity " + filterStr + ")" + ((id == null) ? "" : (" WHERE ID(en1) = " + id)) + " RETURN collect(ID(en1)) AS result;";
	console.log(checkForExistQuery + "\n");
	var result = [];
	session.run(checkForExistQuery).subscribe({
		onNext: function(record) {
			result = record.get("result");
		},
		onCompleted: function(record) {
			for (var i = 0; i < result.length; i++)
				result[i] = result[i].toNumber();
			if (result.length == 0)
				res.json({message: "Не було знайдено об\'єкти для видалення"});
			else {
				var deleteQuery1 = "UNWIND {ids} AS id1 MATCH (manager:Individual)-[c1:MANAGER]->(en1:LegalEntity) WHERE ID(en1) = id1 DELETE c1;";
				console.log(deleteQuery1 + "\n");
				session.run(deleteQuery1, {ids: result}).catch(standartErrorSend).then(function() {
					var deleteQuery2 = "UNWIND {ids} AS id1 MATCH (en1:LegalEntity)-[:POSESSES]->(lic1:License) WHERE ID(en1) = id1 DETACH DELETE lic1;";
					console.log(deleteQuery2 + "\n");
					session.run(deleteQuery2, {ids: result}).catch(standartErrorSend).then(function() {
						var deleteQuery3 = "UNWIND {ids} AS id1 MATCH (en1:LegalEntity)-[:PARENT]->(aff1:LegalEntity) WHERE ID(en1) = id1 DETACH DELETE aff1;";
						console.log(deleteQuery3 + "\n");
						session.run(deleteQuery3, {ids: result}).catch(standartErrorSend).then(function() {
							session.run("UNWIND {ids} AS id1 MATCH (en1:LegalEntity) WHERE ID(en1) = id1 DELETE en1;", {ids: result}).catch(standartErrorSend).then(function() {
								res.json({message: "Успішно видалено"});
							})
						});
					});
				});
			}
		},
		onError: standartErrorSend
	});
};

module.exports.getStats = function(req, res, session) {
	var entitiesCount = 0, affiliatesCount = 0;
	var serviceTypeStats = {}, certifiedDateStats = {yearly: {minValue: Infinity, maxValue: 0, data: {}, minYear: null},
													monthly: {minValue: Infinity, maxValue: 0, data: {}}};
	var entityTypeStats = {};
	var query1 = "MATCH (a:LegalEntity) WITH collect(DISTINCT a.service_type) AS service_types, count(a) AS total \
UNWIND service_types as type1 MATCH (b:LegalEntity {service_type: type1}) RETURN total, \"s\" + type1 AS typex, count(b) AS amount \
UNION ALL \
MATCH (c:LegalEntity) WITH collect(DISTINCT c.entity_type) AS entity_types, count(c) AS total \
UNWIND entity_types as type2 MATCH (d:LegalEntity {entity_type: type2}) RETURN total, \"e\" + type2 AS typex, count(d) AS amount;";
	session.run(query1).subscribe({
		onNext: function(record) {
			entitiesCount = record.get("total").toNumber();
			var akaType = record.get("typex");
			var count = record.get("amount").toNumber();
			var whereToWrite = (akaType[0] == "s") ? serviceTypeStats : entityTypeStats;
			whereToWrite[akaType.substr(1, akaType.length - 1)] = count;
		},
		onCompleted: function() {
			var query2 = "MATCH (a2:LegalEntity)-[:PARENT]->(a:LegalEntity) \
WITH collect(DISTINCT a.service_type) AS service_types, count(a) AS total UNWIND service_types as type1 \
MATCH (b2:LegalEntity)-[:PARENT]->(b:LegalEntity {service_type: type1}) RETURN total, \"s\" + type1 AS typex, count(b) AS amount \
UNION ALL \
MATCH (c2:LegalEntity)-[:PARENT]->(c:LegalEntity) WITH collect(DISTINCT c.entity_type) AS entity_types, count(c) AS total \
UNWIND entity_types as type2 MATCH (d2:LegalEntity)-[:PARENT]->(d:LegalEntity {entity_type: type2}) RETURN total, \"e\" + type2 AS typex, count(d) AS amount;";
			session.run(query2).subscribe({
				onNext: function(record) {
					affiliatesCount = record.get("amount").toNumber();
					var akaType = record.get("typex");
					var count = record.get("amount").toNumber();
					var whereToWrite = (akaType[0] == "s") ? serviceTypeStats : entityTypeStats;
					whereToWrite[akaType.substr(1, akaType.length - 1)] -= count;
				},
				onCompleted: function() {
					entitiesCount -= affiliatesCount;
					var k1 = new Date();
					var minYear = k1.getFullYear() + 1, minCount = Infinity, maxCount = 0;
					var yearStatsQuery1 = "MATCH (a:LegalEntity) WITH DISTINCT(toInt(split(a.certified_date, \".\")[2])) as year \
MATCH (b:LegalEntity) WHERE b.certified_date =~ (\".*.\" + toString(year)) RETURN year, count(b) AS c1;";
					session.run(yearStatsQuery1).subscribe({
						onNext: function(record) {
							var year = record.get("year").toNumber();
							var count = record.get("c1").toNumber();
							minYear = Math.min(minYear, year);
							certifiedDateStats.yearly.data[year] = count;
						},
						onCompleted: function() {
							var yearStatsQuery2 = "MATCH (a2:LegalEntity)-[:PARENT]->(a:LegalEntity) \
WITH DISTINCT(toInt(split(a.certified_date, \".\")[2])) as year MATCH (b2:LegalEntity)-[:PARENT]->(b:LegalEntity) \
WHERE b.certified_date =~ (\".*.\" + toString(year)) RETURN year, count(b) AS c1;";
							session.run(yearStatsQuery2).subscribe({
								onNext: function(record) {
									var year = record.get("year").toNumber();
									var count = record.get("c1").toNumber();
									certifiedDateStats.yearly.data[year] -= count;
									var v1 = certifiedDateStats.yearly.data[year];
									minCount = Math.min(minCount, v1);
									maxCount = Math.max(maxCount, v1);
								},
								onCompleted: function() {
									certifiedDateStats.yearly.minValue = minCount;
									certifiedDateStats.yearly.maxValue = maxCount;
									certifiedDateStats.yearly.minYear = minYear;
									minCount = Infinity; maxCount = 0;
									var monthStatsQuery1 = "MATCH (a:LegalEntity) \
WITH DISTINCT(toInt(split(a.certified_date, \".\")[1])) as month MATCH (b:LegalEntity) \
WHERE b.certified_date =~ (\".*\" + toString(month) + \".\" + toString({year})) RETURN month, count(b) AS c1;";
									console.log(monthStatsQuery1);
									session.run(monthStatsQuery1, {year: k1.getFullYear()}).subscribe({
										onNext: function(record) {
											var month = record.get("month").toNumber();
											var count = record.get("c1").toNumber();
											certifiedDateStats.monthly.data[month] = count;
											console.log(month);
											console.log(count);
										},
										onCompleted: function() {
											var monthStatsQuery2 = "MATCH (a2:LegalEntity)-[:PARENT]->(a:LegalEntity) \
WITH DISTINCT(toInt(split(a.certified_date, \".\")[1])) as month MATCH (b2:LegalEntity)-[:PARENT]->(b:LegalEntity) \
WHERE b.certified_date =~ (\".*\" + toString(month) + \".\" + toString({year})) RETURN month, count(b) AS c1;";
											console.log(monthStatsQuery2);
											session.run(monthStatsQuery2, {year: k1.getFullYear()}).subscribe({
												onNext: function(record) {
													var month = record.get("month").toNumber();
													var count = record.get("c1").toNumber();
													certifiedDateStats.monthly.data[month] -= count;
													var v1 = certifiedDateStats.monthly.data[month];
													minCount = Math.min(minCount, v1);
													maxCount = Math.max(maxCount, v1);
													console.log(month);
													console.log(count);
												},
												onCompleted: function() {
													certifiedDateStats.monthly.minValue = minCount;
													certifiedDateStats.monthly.maxValue = maxCount;
													minCount = Infinity; maxCount = 0;
													console.log(certifiedDateStats);
													res.json({"entitiesCount": entitiesCount, "affiliatesCount": affiliatesCount, "certifiedDateStats": certifiedDateStats, "entityTypeStats": entityTypeStats, "serviceTypeStats": serviceTypeStats});
												},
												onError: standartErrorSend
											});
										},
										onError: standartErrorSend
									});
								},
								onError: standartErrorSend
							});
						},
						onError: standartErrorSend
					});
				},
				onError: standartErrorSend
			});
		},
		onError: standartErrorSend
	});
}