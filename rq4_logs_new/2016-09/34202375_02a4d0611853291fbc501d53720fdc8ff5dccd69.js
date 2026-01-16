var this_year =[
    0, //0w
	150, //1w
	1092, //2w
	1031, //3w	
	1002, //4w
	820, //5w
	334, //6w
	168, //7w
	852, //8w
	1129, //9w	
	1668, //10w
	1586, //11w
	1803, //12w
	1507, //13w
	1253, //14w
	1167, //15w
	1780, //16w
	3897, //17w
	1335, //18w
	1000, //19w
	1208, //20w
	1302, //21w
	1096, //22w
    1007, //23w
    784, //24w，端午假期
    1172, //25w
    2945, //26w
    2306, //27w
    1156, //28w
    1399, //29w
    1583, //30w
    1897, //31w
    1276, //32w
    1206, //33w
    1606, //34w
    1358, //35w
    1163, //36w
    1069, //37w
    888, //38w
];

var this_year_pv = [
    0,
	288208, //1w
	776331, //2w
	838490, //3w
	771156, //4w
	567547, //5w
	278760, //6w
	141800, //7w
	497191, //8w	
	716591, //9w
	865971, //10w
	774273, //11w
	802883, //12w
	808615, //13w
	687814, //14w
	667902, //15w
	787220, //16w
	771084, //17w
	699257, //18w
	616074, //19w
	733405, //20w
	763861, //21w`
	767074, //22w
    709524, //23w
    670233, //24w
    1041209, //25w
    918772, //26w
    699852, //27w
    1013595, //28w
    836859, //29w
    1145328, //30w
    937595, //31w
    785733, //32w
    906294, //33w
    1037369, //34w
    796827, //35w
    739878, //36w
    1260855, //37w
    1144681, //38w
];

weeks = 53;

var axis_x = [];
for (i=0;i<=weeks;i++){
    axis_x[i] = i;
}


function avr_metric(metric){
    var ever_week = []
    for(i=0;i<weeks;i++){
        ever_week[i] = (metric/weeks)*i;
    }
    return ever_week;
}

function sumWeeks(x){
    for(i=0;i<x.length;i++){
        if(i !== 0){
            x[i] += x[i-1];
        }else{
            x[i] = 0;
        }
    }
    return x;
}

var avr_dream = avr_metric(75909);
// var avr_base = avr_metric(69540)
var avr_dream_pv = avr_metric(47265261)



        // 路径配置
        require.config({
            paths: {
                echarts: 'http://echarts.baidu.com/build/dist'
            }
        });
        
        // 使用
        require(
            [
                'echarts',
                'echarts/chart/line' // 使用柱状图就加载line模块，按需加载
            ],
            function (ec) {
                // 基于准备好的dom，初始化echarts图表
                var myChart = ec.init(document.getElementById('main')); 
                var myChart2 = ec.init(document.getElementById('PV_wrap'))
               //询价的配置 
                var option = {
                    color:['#73c1fb',"rgba(191,239,255,1)","#ff6839","#c1c1c1"],
                    title:{
                        text:'询价量'
                    },
                    tooltip: {
                        show: true,
                        trigger:'axis'
                    },
                    grid:{
                        containLabel:true,
                        
                    },
                    legend: {
                        data:['Dream','Base拆分','2016','2015'],
                    },
                    xAxis : [
                        {
                            type : 'category',
                            boundaryGap:false,
                            data : axis_x,
                            splitLine:{
                                show:false
                            }
                        }
                    ],
                    yAxis : [
                        {
                            type : 'value',
                            splitLine:{
                                show:false
                            },
                            show:false
                        }
                    ],
                    series : [
                        {
                            "name":"Dream",
                            "type":"line",
                            symbol:'none',
                            "data":avr_dream,
                            itemStyle:{
                                normal:{
                                    lineStyle:{
                                        color:"#73c1fb"
                                        
                                    }
                                }
                            }
                            
                        },
                        {
                            "name":"2016",
                            "type":"line",
                            symbol:'none',
                            "data":sumWeeks(this_year),                         
                            itemStyle:{
                                normal:{
                                    lineStyle:{
                                        color:"#ff6839"
                                    }
                                }
                            }
                        },
                        {
                            "name":"2015",
                            "type":"line",
                            symbol:'none',
                            "data":sumWeeks(last_yearXunjia),
                            itemStyle:{
                                normal:{
                                    lineStyle:{
                                        color:"#c1c1c1"
                                    }
                                }
                            }
                        },
                        {
                            "name":"Base拆分",
                            "type":"line",
                            symbol:'none',
                            "data":sumWeeks(askPriceSlide),
                            itemStyle:{
                                normal:{
                                    lineStyle:{
                                        // color:"#987e2f"
                                    }
                                }
                            }
                        },
                    ],
                };
                //PV的配置
                var option2 = {
                    color:['#73c1fb',"rgba(191,239,255,1)","#ff6839","#c1c1c1"],
                    title: {
                        text:'整站PV',
                    },   
                    tooltip: {
                        show: true,
                        trigger:'axis'
                    },
                    grid:{
                        containLabel:true,
                        left: '3%',
                        show:false
                    },
                    legend: {
                        data:['Dream','Base拆分','2016','2015'],
                    },
                    xAxis : [
                        {
                            type : 'category',
                            boundaryGap:false,
                            data : axis_x,
                            splitLine:{
                                show:false
                            }
                        }
                    ],
                    yAxis : [
                        {
                            type : 'value',
                            splitLine:{
                                show:false
                            },
                            show:false
                        }
                    ],
                    series : [
                        {
                            "name":"Dream",
                            "type":"line",
                            symbol:'none',
                            "data":avr_dream_pv,
                            itemStyle:{
                                normal:{
                                    lineStyle:{
                                        color:"#73c1fb"
                                    }
                                }
                            }
                            
                        },
                        {
                            "name":"2016",
                            "type":"line",
                            symbol:'none',
                            "data":sumWeeks(this_year_pv),
                            itemStyle:{
                                normal:{
                                    lineStyle:{
                                        color:"#ff6839"
                                    }
                                }
                            }
                        },
                        {
                            "name":"2015",
                            "type":"line",
                            "data":sumWeeks(last_yearPV),
                            symbol:'none',
                            itemStyle:{
                                normal:{
                                    lineStyle:{
                                        color:"#c1c1c1"
                                    }
                                }
                            }
                        },
                        {
                            "name":"Base拆分",
                            "type":"line",
                            symbol:'none',
                            "data":sumWeeks(pv_slide),
                            itemStyle:{
                                normal:{
                                    lineStyle:{
                                        // color:"#987e2f"
                                    }
                                }
                            }
                        },
                    ],
                };
        
                // 为echarts对象加载数据 
                myChart.setOption(option); 
                myChart2.setOption(option2);
            }
        );
        
var askPriceSlide = [
	248,
	1200,
	1732,
	1208,
	1207,
	956,
	653,
	282,
	609,
	1310,
	1268,
	1405,
	1571,
	2310,
	1547,
	1511,
	1888,
	1360,
	1948,
	1385,
	1247,
	1335,
	1295,
	1268,
	1048,
	1053,
	1031,
	1515,
	1413,
	1325,
	1170,
	1042,
	1450,
	1210,
	1178,
	952,
	1288,
	1339,
	1300,
	817,
	1120,
	1597,
	1424,
	1551,
	1529,
	1457,
	1471,
	1533,
	1449,
	1693,
	1685,
	1391,
	1892,
 ];
 
var pv_slide = [
    129788,
	743753,
	731440,
	703892,
	700746,
	616186,
	469532,
	195118,
	408635,
	715099,
	809889,
	865658,
	883880,
	836478,
	751705,
	847216,
	895189,
	779120,
	996304,
	979125,
	1005053,
	948405,
	923045,
	925816,
	823901,
	743924,
	835394,
	943058,
	962954,
	974415,
	894026,
	817673,
	709545,
	727297,
	839797,
	695022,
	908711,
	963178,
	865990,
	584101,
	695402,
	926254,
	1035602,
	1035602,
	1035602,
	1035602,
	1035602,
	1027604,
	994539,
	1035602,
	1035602,
	1035602,
	1035602,
];

 
var last_yearXunjia = [
    128,
    908,
    1344,
    916,
    913,
    708,
    460,
    156,
    424,
    998,
    964,
    1076,
    1212,
    1818,
    1193,
    1163,
    1472,
    1039,
    1521,
    1060,
    947,
    1019,
    986,
    964,
    783,
    788,
    770,
    1166,
    1083,
    1011,
    884,
    779,
    1113,
    916,
    890,
    705,
    980,
    1022,
    990,
    594,
    843,
    1234,
    1092,
    1196,
    1178,
    1119,
    1130,
    1181,
    1112,
    2952,
    1306,
    1065,
    3803
];

var last_yearPV = [
    67276,
    505822,
    497027,
    477350, 
    475103, 
    414703, 
    309950, 
    113940, 
    266452, 
    485355, 
    553062,
    592897, 
    605913,
    572054,
    511502,
    579724,
    613991,
    531084,
    686261,
    673945,
    692465,
    652002,
    633888,
    635867,
    563071,
    505944,
    571280,
    648183,
    662394,
    670581,
    613160,
    558622,
    481388,
    494068,
    574425,
    471014,
    623649,
    662554,
    593134,
    391785,
    471286,
    636180,
    807427,
    803934,
    814506,
    982936,
    774489,
    708573,
    684955,
    1027759,
    758579,
    953885,
    852824,
    ];