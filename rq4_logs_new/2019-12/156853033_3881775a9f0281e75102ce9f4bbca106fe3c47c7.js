db.getCollection('secao_votacao_gerais_2018').find({"NR_TURNO": "1", "NR_VOTAVEL": {"$in": ["95", "96"]} }).count() // 78653 | 3912

db.getCollection('secao_votacao_gerais_2018').find({"NR_TURNO": "1", "NR_VOTAVEL": {"$nin": ["95", "96"]}, "CD_CARGO": { "$in": ["7"]} })

db.getCollection('secao_votacao_gerais_2018').distinct("CD_CARGO")

// 6,7,5,3

db.getCollection('secao_votacao_gerais_2018').find({"NR_TURNO": "1", "CD_CARGO": { "$in": ["3"]}, "NR_PARTIDO": "50" }).count() // 5576 | PSOL: 485

db.getCollection('secao_votacao_gerais_2018').find({"NR_TURNO": "1", "CD_CARGO": { "$in": ["5"]}, "NR_PARTIDO": "50" }).count() // 9233 | PSOL: 963

db.getCollection('secao_votacao_gerais_2018').find({"NR_TURNO": "1", "CD_CARGO": { "$in": ["6"]}, "NR_PARTIDO": "50" }).count() // 34601 | PSOL: 1873 

db.getCollection('secao_votacao_gerais_2018').find({"NR_TURNO": "1", "CD_CARGO": { "$in": ["7"]}, "NR_PARTIDO": "50" }).count() // 33155 | PSOL: 2078

// 3: Governador / 5: Senado / 6: Dep Federal / 7: Dep Estadual