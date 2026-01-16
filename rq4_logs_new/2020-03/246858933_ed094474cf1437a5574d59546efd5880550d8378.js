var modal = document.getElementById("calc");
var btn = document.getElementById("myBtn");
var val1;
var val2;
var val3;
var val4;
var rent= {
   "Undeveloped Land" :{
    "Downtown Metro":{
        "Small Cell Site":{
            "Indoor Small Cell":" n/a ",
            "Outdoor Small Cell":" $300 ",
            "Small Cell On New Pole":" $350 ",
            "Small Cell On Structure":" n/a "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,300 ",
            "Roof Top Cell Site":" n/a ",
            "Cell Site On Structure":" n/a "
    
        },
        "Fiber":{
            "Fiber Hut":" $300 ",
            "Fiber Terminal":" $125 "  
        },
        "Indoor Equipment":{
            "Mini Data Center":"n/a",
            "Dedicated Room":"n/a"
        }
    },
    "Urban":{
        "Small Cell Site":{
            "Indoor Small Cell":" n/a ",
            "Outdoor Small Cell":" $275 ",
            "Small Cell On New Pole":" $325 ",
            "Small Cell On Structure":" n/a "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,175 ",
            "Roof Top Cell Site":" n/a ",
            "Cell Site On Structure":" n/a "
        },
        "Fiber":{
            "Fiber Hut":" $250 ",
            "Fiber Terminal":" $100 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"n/a",
            "Dedicated Room":"n/a"
        }
    },
    "Suburban":{
        "Small Cell Site":{
            "Indoor Small Cell":" n/a ",
            "Outdoor Small Cell":" $250 ",
            "Small Cell On New Pole":" $300 ",
            "Small Cell On Structure":" n/a "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,050 ",
            "Roof Top Cell Site":" n/a ",
            "Cell Site On Structure":" n/a "
        },
        "Fiber":{
            "Fiber Hut":" $150 ",
            "Fiber Terminal":" $75 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"n/a",
            "Dedicated Room":"n/a"
        }
    },
    "Small City":{
        "Small Cell Site":{
            "Indoor Small Cell":" n/a ",
            "Outdoor Small Cell":" $250 ",
            "Small Cell On New Pole":" $300 ",
            "Small Cell On Structure":" n/a "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,050 ",
            "Roof Top Cell Site":" n/a ",
            "Cell Site On Structure":" n/a "
        },
        "Fiber":{
            "Fiber Hut":" $150 ",
            "Fiber Terminal":" $75 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"n/a",
            "Dedicated Room":"n/a"
        }
    },
    "Rural":{
        "Small Cell Site":{
            "Indoor Small Cell":" n/a ",
            "Outdoor Small Cell":" $200 ",
            "Small Cell On New Pole":" $250 ",
            "Small Cell On Structure":" n/a "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $800 ",
            "Roof Top Cell Site":" n/a ",
            "Cell Site On Structure":" n/a " 
        },
        "Fiber":{
            "Fiber Hut":" $100 ",
            "Fiber Terminal":" $50 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"n/a",
            "Dedicated Room":"n/a"
        }
    }
   },
   "Residential":{
    "Downtown Metro":{
        "Small Cell Site":{
            "Indoor Small Cell":"$250",
            "Outdoor Small Cell":" $300 ",
            "Small Cell On New Pole":" $350 ",
            "Small Cell On Structure":" $400 "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,250 ",
            "Roof Top Cell Site":" $1,500 ",
            "Cell Site On Structure":" $2000 "
    
        },
        "Fiber":{
            "Fiber Terminal":" $125 ",
            "Fiber Hut":" $300 "  
        },
        "Indoor Equipment":{
            "Mini Data Center":"$800",
            "Dedicated Room":"$2500"
        }
    },
    "Urban":{
        "Small Cell Site":{
            "Indoor Small Cell":" $225 ",
            "Outdoor Small Cell":" $275 ",
            "Small Cell On New Pole":" $325 ",
            "Small Cell On Structure":" $325 "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,125 ",
            "Roof Top Cell Site":" $1,350 ",
            "Cell Site On Structure":" $1,850"
        },
        "Fiber":{
            "Fiber Hut":" $250 ",
            "Fiber Terminal":" $100 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$700",
            "Dedicated Room":"$2000"
        }
    },
    "Suburban":{
        "Small Cell Site":{
            "Indoor Small Cell":" $200 ",
            "Outdoor Small Cell":" $250 ",
            "Small Cell On New Pole":" $300 ",
            "Small Cell On Structure":" $350 "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,000 ",
            "Roof Top Cell Site":" $1200 ",
            "Cell Site On Structure":" $1700"
        },
        "Fiber":{
            "Fiber Hut":" $250 ",
            "Fiber Terminal":" $75 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$600",
            "Dedicated Room":"$1500"
        }
    },
    "Small City":{
        "Small Cell Site":{
            "Indoor Small Cell":" $200 ",
            "Outdoor Small Cell":" $250 ",
            "Small Cell On New Pole":" $300 ",
            "Small Cell On Structure":" $350 "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,000 ",
            "Roof Top Cell Site":" $1200 ",
            "Cell Site On Structure":" $1700 "
        },
        "Fiber":{
            "Fiber Hut":" $250 ",
            "Fiber Terminal":" $75 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$600",
            "Dedicated Room":"$1500"
        }
    },
    "Rural":{
        "Small Cell Site":{
            "Indoor Small Cell":" $150 ",
            "Outdoor Small Cell":" $200 ",
            "Small Cell On New Pole":" $250 ",
            "Small Cell On Structure":" $300"
        },
        "Full Size Cell Site":{
            "Cell Tower":" $750 ",
            "Roof Top Cell Site":" $1000 ",
            "Cell Site On Structure":" $1500 "
        },
        "Fiber":{
            "Fiber Hut":" $200 ",
            "Fiber Terminal":" $50 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$400",
            "Dedicated Room":"$1000"
        }
    }
   },
   "Office Building/MDU":{
    "Downtown Metro":{
        "Small Cell Site":{
            "Indoor Small Cell":"$350",
            "Outdoor Small Cell":" $400 ",
            "Small Cell On New Pole":" $450 ",
            "Small Cell On Structure":" $500 "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,350 ",
            "Roof Top Cell Site":" $3,000 ",
            "Cell Site On Structure":" $2,250 "
        },
        "Fiber":{
            "Fiber Hut":" $350 " ,  
            "Fiber Terminal":" $175 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$1000",
            "Dedicated Room":"$3000"
        }
    },
    "Urban":{
        "Small Cell Site":{
            "Indoor Small Cell":" $325 ",
            "Outdoor Small Cell":" $375 ",
            "Small Cell On New Pole":" $425 ",
            "Small Cell On Structure":" $475 "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,225 ",
            "Roof Top Cell Site":" $2,700 ",
            "Cell Site On Structure":" $2,100"
        },
        "Fiber":{
            "Fiber Hut":" $300 ",
            "Fiber Terminal":" $150 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$800",
            "Dedicated Room":"$2500"
        }
    },
    "Suburban":{
        "Small Cell Site":{
            "Indoor Small Cell":" $300 ",
            "Outdoor Small Cell":" $350 ",
            "Small Cell On New Pole":" $400 ",
            "Small Cell On Structure":" $450 "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,100 ",
            "Roof Top Cell Site":" $2400 ",
            "Cell Site On Structure":" $1950"
        },
        "Fiber":{
            "Fiber Hut":" $300 ",
            "Fiber Terminal":" $125 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$700",
            "Dedicated Room":"$1700"
        }
    },
    "Small City":{
        "Small Cell Site":{
            "Indoor Small Cell":" $300 ",
            "Outdoor Small Cell":" $350 ",
            "Small Cell On New Pole":" $400 ",
            "Small Cell On Structure":" $450 "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,100 ",
            "Roof Top Cell Site":" $2400 ",
            "Cell Site On Structure":" $1950 "
        },
        "Fiber":{
            "Fiber Hut":" $300 ",
            "Fiber Terminal":" $125 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$700",
            "Dedicated Room":"$1700"
        }
    },
    "Rural":{
        "Small Cell Site":{
            "Indoor Small Cell":" $250 ",
            "Outdoor Small Cell":" $300 ",
            "Small Cell On New Pole":" $350 ",
            "Small Cell On Structure":" $400"
        },
        "Full Size Cell Site":{
            "Cell Tower":" $850 ",
            "Roof Top Cell Site":" $2000 ",
            "Cell Site On Structure":" $1750 "
        },
        "Fiber":{
            "Fiber Hut":" $250 ",
            "Fiber Terminal":" $100 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$500",
                "Dedicated Room":"$1250"
        }
    }
   },
   "Industrial/Farm":{
    "Downtown Metro":{
        "Small Cell Site":{
            "Indoor Small Cell":"$300",
            "Outdoor Small Cell":" $350 ",
            "Small Cell On New Pole":" $400 ",
            "Small Cell On Structure":" $450 "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,325 ",
            "Roof Top Cell Site":" $2,500 ",
            "Cell Site On Structure":" $2,200 "
        },
        "Fiber":{
            "Fiber Hut":" $325 " ,  
            "Fiber Terminal":" $150 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$900",
            "Dedicated Room":"$2700"
        }
    },
    "Urban":{
        "Small Cell Site":{
            "Indoor Small Cell":" $275 ",
            "Outdoor Small Cell":" $325 ",
            "Small Cell On New Pole":" $375 ",
            "Small Cell On Structure":" $425 "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,200 ",
            "Roof Top Cell Site":" $2,200 ",
            "Cell Site On Structure":" $2,050"
        },
        "Fiber":{
            "Fiber Hut":" $275 ",
            "Fiber Terminal":" $125 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$800",
            "Dedicated Room":"$2200"
        }
    },
    "Suburban":{
        "Small Cell Site":{
            "Indoor Small Cell":" $250 ",
            "Outdoor Small Cell":" $300 ",
            "Small Cell On New Pole":" $350 ",
            "Small Cell On Structure":" $400 "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,075 ",
            "Roof Top Cell Site":" $1900 ",
            "Cell Site On Structure":" $1900"
        },
        "Fiber":{
            "Fiber Hut":" $275 ",
            "Fiber Terminal":" $100 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$700",
            "Dedicated Room":"$1600"
        }
    },
    "Small City":{
        "Small Cell Site":{
            "Indoor Small Cell":" $250 ",
            "Outdoor Small Cell":" $300 ",
            "Small Cell On New Pole":" $350 ",
            "Small Cell On Structure":" $400 "
        },
        "Full Size Cell Site":{
            "Cell Tower":" $1,075 ",
                    "Roof Top Cell Site":" $1900 ",
                    "Cell Site On Structure":" $1900 "
        },
        "Fiber":{
            "Fiber Hut":" $275 ",
            "Fiber Terminal":" $100 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$700",
            "Dedicated Room":"$1500"
        }
    },
    "Rural":{
        "Small Cell Site":{
            "Indoor Small Cell":" $200 ",
            "Outdoor Small Cell":" $250 ",
            "Small Cell On New Pole":" $300 ",
            "Small Cell On Structure":" $350"
        },
        "Full Size Cell Site":{
            "Cell Tower":" $825 ",
            "Roof Top Cell Site":" $1500 ",
            "Cell Site On Structure":" $1700 "
        },
        "Fiber":{
            "Fiber Hut":" $225 ",
            "Fiber Terminal":" $75 "
        },
        "Indoor Equipment":{
            "Mini Data Center":"$450",
            "Dedicated Room":"$1000"
        }
    }
   }
}  





var current=   {
   "Undeveloped Land" :{
    "Downtown Metro":{
        "Small Cell Site":{
           "Indoor Small Cell":" n/a ", 
           "Outdoor Small Cell":" $50,400 ", 
           "Small Cell On New Pole":" $58,800 ", 
           "Small Cell On Structure":" n/a "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $218,400 ", 
           "Roof Top Cell Site":" n/a ", 
           "Cell Site On Structure":" n/a "
    
        },
        "Fiber":{
           "Fiber Terminal":" $15,000 ", 
  "Fiber Hut":" $36,000 "
        },
        "Indoor Equipment":{
           "Mini Data Center":"n/a", 
           "Dedicated Room":"n/a"
        }
    },
    "Urban":{
        "Small Cell Site":{
           "Indoor Small Cell":" n/a ", 
  "Outdoor Small Cell":" $46,200 ", 
  "Small Cell On New Pole":" $54,600 ", 
  "Small Cell On Structure":" n/a "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $197,400 ", 
  "Roof Top Cell Site":" n/a ", 
  "Cell Site On Structure":" n/a "
        },
        "Fiber":{
           "Fiber Terminal":" $12,000 ", 
  "Fiber Hut":" $30,000 "
        },
        "Indoor Equipment":{
           "Mini Data Center":"n/a", 
           "Dedicated Room":"n/a"
        }
    },
    "Suburban":{
        "Small Cell Site":{
           "Indoor Small Cell":" n/a ", 
           "Outdoor Small Cell":" $42,000 ", 
           "Small Cell On New Pole":" $50,400 ", 
           "Small Cell On Structure":" n/a "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $176,400 ", 
           "Roof Top Cell Site":" n/a ", 
           "Cell Site On Structure":" n/a "
        },
        "Fiber":{
           "Fiber Terminal":" $9,000 ", 
  "Fiber Hut":" $18,000 "
        },
        "Indoor Equipment":{
           "Mini Data Center":"n/a", 
           "Dedicated Room":"n/a"
        }
    },
    "Small City":{
        "Small Cell Site":{
           "Indoor Small Cell":" n/a ", 
           "Outdoor Small Cell":" $42,000 ", 
           "Small Cell On New Pole":" $50,400 ", 
           "Small Cell On Structure":" n/a "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $176,400 ", 
  "Roof Top Cell Site":" n/a ", 
  "Cell Site On Structure":" n/a "
        },
        "Fiber":{
           "Fiber Terminal":" $9,000", 
           "Fiber Hut":" $18,000 "
        },
        "Indoor Equipment":{
           "Mini Data Center":"n/a", 
  "Dedicated Room":"n/a"

        }
    },
    "Rural":{
        "Small Cell Site":{
           "Indoor Small Cell":" n/a ", 
           "Outdoor Small Cell":" $33,600 ", 
           "Small Cell On New Pole":" $42,000 ", 
           "Small Cell On Structure":" n/a "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $134,400 ", 
           "Roof Top Cell Site":" n/a ", 
           "Cell Site On Structure":" n/a "
        },
        "Fiber":{
           "Fiber Terminal":" $6,000 ", 
  "Fiber Hut":" $12,000"
        },
        "Indoor Equipment":{
           "Mini Data Center":"n/a", 
           "Dedicated Room":"n/a"
        }
    }
   },
   "Residential":{
    "Downtown Metro":{
        "Small Cell Site":{
           "Indoor Small Cell":"$42,000", 
           "Outdoor Small Cell":" $50,400", 
           "Small Cell On New Pole":" $58,800 ", 
           "Small Cell On Structure":"$67,200"
        },
        "Full Size Cell Site":{
           "Cell Tower":" $210,000 ", 
           "Roof Top Cell Site":"$252,000", 
           "Cell Site On Structure":"$336,000"
    
        },
        "Fiber":{
           "Fiber Terminal":" $15,000", 
  "Fiber Hut":" $36,000 " 
        },
        "Indoor Equipment":{
           "Mini Data Center":"$96,000", 
           "Dedicated Room":"$300,000"
        }
    },
    "Urban":{
        "Small Cell Site":{
           "Indoor Small Cell":" $37,800 ", 
           "Outdoor Small Cell":" $46,200 ", 
           "Small Cell On New Pole":" $54,600 ", 
           "Small Cell On Structure":" $63,000 "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $189,000 ", 
           "Roof Top Cell Site":" $226,800 ", 
           "Cell Site On Structure":" $310,800 "
        },
        "Fiber":{
           "Fiber Terminal":" $12,000 ", 
  "Fiber Hut":" $30,000 "
        },
        "Indoor Equipment":{
           "Mini Data Center":"$84,000", 
           "Dedicated Room":"$240,000"
        }
    },
    "Suburban":{
        "Small Cell Site":{
           "Indoor Small Cell":" $33,600 ", 
           "Outdoor Small Cell":" $42,000 ", 
           "Small Cell On New Pole":" $50,400 ", 
           "Small Cell On Structure":" $58,800 "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $168,000 ", 
           "Roof Top Cell Site":" $201,600 ", 
           "Cell Site On Structure":" $285,600"
        },
        "Fiber":{
           "Fiber Terminal":" $9,000 ", 
           "Fiber Hut":" $30,000"
        },
        "Indoor Equipment":{
           "Mini Data Center":"$72,000", 
           "Dedicated Room":"$180,000"
        }
    },
    "Small City":{
        "Small Cell Site":{
           "Indoor Small Cell":" $33,600 ", 
           "Outdoor Small Cell":" $42,000 ", 
           "Small Cell On New Pole":" $50,400 ", 
           "Small Cell On Structure":" $58,800 "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $168,000 ", 
           "Roof Top Cell Site":" $201,600 ", 
           "Cell Site On Structure":" $285,600 "
        },
        "Fiber":{
           "Fiber Terminal":" $9,000 ", 
           "Fiber Hut":" $30,000 "
        },
        "Indoor Equipment":{
           "Mini Data Center":"$72,000", 
           "Dedicated Room":"$180,000"
        }
    },
    "Rural":{
        "Small Cell Site":{
           "Indoor Small Cell":" $25,200", 
  "Outdoor Small Cell":" $33,600", 
  "Small Cell On New Pole":" $42,000 ", 
  "Small Cell On Structure":" $50,400 "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $126,000 ", 
  "Roof Top Cell Site":" $168,000", 
  "Cell Site On Structure":" $252,000 "
        },
        "Fiber":{
           "Fiber Terminal":" $6,000", 
  "Fiber Hut":" $24,000"
        },
        "Indoor Equipment":{
           "Mini Data Center":"$48,000", 
           "Dedicated Room":"$120,000"
        }
    }
   },
   "Office Building/MDU":{
    "Downtown Metro":{
        "Small Cell Site":{
           "Indoor Small Cell":" $58,800 ", 
           "Outdoor Small Cell":" $67,200 ", 
           "Small Cell On New Pole":" $75,600 ", 
           "Small Cell On Structure":" $84,000 "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $226,800 ", 
           "Roof Top Cell Site":" $504,000", 
           "Cell Site On Structure":" $378,000 "
        },
        "Fiber":{
           "Fiber Terminal":" $21,000 ", 
  "Fiber Hut":" $42,000 "
        },
        "Indoor Equipment":{
           "Mini Data Center":"$120,000", 
           "Dedicated Room":"$360,000"
        }
    },
    "Urban":{
        "Small Cell Site":{
           "Indoor Small Cell":" $54,600 ", 
           "Outdoor Small Cell":" $63,000 ", 
           "Small Cell On New Pole":" $71,400 ", 
           "Small Cell On Structure":" $79,800 "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $205,800 ", 
  "Roof Top Cell Site":"$453,600  ", 
  "Cell Site On Structure":" $352,800"
        },
        "Fiber":{
           "Fiber Terminal":" $18,000 ", 
           "Fiber Hut":" $36,000"
        },
        "Indoor Equipment":{
           "Mini Data Center":"$96,000", 
  "Dedicated Room":"$300,000"
        }
    },
    "Suburban":{
        "Small Cell Site":{
           "Indoor Small Cell":" $50,400 ", 
           "Outdoor Small Cell":" $58,800 ", 
           "Small Cell On New Pole":" $67,200 ", 
           "Small Cell On Structure":" $75,600 "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $184,800 ", 
           "Roof Top Cell Site":" $403,200 ", 
           "Cell Site On Structure":" $327,600 "
        },
        "Fiber":{
           "Fiber Terminal":" $15,000 ", 
           "Fiber Hut":" $36,000 "
        },
        "Indoor Equipment":{
           "Mini Data Center":"$84,000", 
  "Dedicated Room":"$204,000"
        }
    },
    "Small City":{
        "Small Cell Site":{
           "Indoor Small Cell":" $50,400 ", 
           "Outdoor Small Cell":" $58,800 ", 
           "Small Cell On New Pole":" $67,200 ", 
           "Small Cell On Structure":" $75,600 "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $184,800 ", 
           "Roof Top Cell Site":" $403,200 ", 
           "Cell Site On Structure":" $327,600 "
        },
        "Fiber":{
           "Fiber Terminal":" $15,000 ", 
           "Fiber Hut":" $36,000 "
        },
        "Indoor Equipment":{
           "Mini Data Center":"$84,000", 
           "Dedicated Room":"$204,000"
        }
    },
    "Rural":{
        "Small Cell Site":{
           "Indoor Small Cell":" $42,000", 
  "Outdoor Small Cell":" $50,400 ", 
  "Small Cell On New Pole":" $58,800 ", 
  "Small Cell On Structure":" $67,200"
        },
        "Full Size Cell Site":{
           "Cell Tower":" $142,800 ", 
  "Roof Top Cell Site":" $336,000 ", 
  "Cell Site On Structure":" $294,000 "
        },
        "Fiber":{
           "Fiber Terminal":" $12,000 ", 
           "Fiber Hut":" $30,000 "
        },
        "Indoor Equipment":{
           "Mini Data Center":"$60,000", 
           "Dedicated Room":"$150,000"
        }
    }
   },
   "Industrial/Farm":{
    "Downtown Metro":{
        "Small Cell Site":{
           "Indoor Small Cell":" $50,400 ", 
           "Outdoor Small Cell":" $58,800 ", 
           "Small Cell On New Pole":" $67,200 ", 
           "Small Cell On Structure":" $75,600 "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $222,600 ", 
           "Roof Top Cell Site":" $420,000 ", 
           "Cell Site On Structure":" $369,600 "
        },
        "Fiber":{
           "Fiber Terminal":" $18,000 ", 
           "Fiber Hut":" $39,000"
        },
        "Indoor Equipment":{
           "Mini Data Center":"$108,000", 
  "Dedicated Room":"$324,000"
        }
    },
    "Urban":{
        "Small Cell Site":{
           "Indoor Small Cell":" $46,200 ", 
  "Outdoor Small Cell":" $54,600 ", 
  "Small Cell On New Pole":" $63,000 ", 
  "Small Cell On Structure":" $71,400"
        },
        "Full Size Cell Site":{
           "Cell Tower":" $201,600 ", 
           "Roof Top Cell Site":" $369,600", 
           "Cell Site On Structure":" $344,400 "
        },
        "Fiber":{
           "Fiber Terminal":" $15,000 ", 
           "Fiber Hut":" $33,000 "
        },
        "Indoor Equipment":{
           "Mini Data Center":"$96,000", 
           "Dedicated Room":"$264,000"
        }
    },
    "Suburban":{
        "Small Cell Site":{
           "Indoor Small Cell":" $42,000 ", 
           "Outdoor Small Cell":" $50,400 ", 
           "Small Cell On New Pole":" $58,800", 
           "Small Cell On Structure":" $67,200"
        },
        "Full Size Cell Site":{
           "Cell Tower":" $180,600 ", 
  "Roof Top Cell Site":" $319,200 ", 
  "Cell Site On Structure":" $319,200 "
        },
        "Fiber":{
           "Fiber Terminal":" $12,000 ", 
           "Fiber Hut":" $33,000 "
        },
        "Indoor Equipment":{
           "Mini Data Center":"$84,000", 
  "Dedicated Room":"$192,000"
        }
    },
    "Small City":{
        "Small Cell Site":{
           "Indoor Small Cell":" $42,000 ", 
           "Outdoor Small Cell":" $50,400 ", 
           "Small Cell On New Pole":" $58,800 ", 
           "Small Cell On Structure":" $67,200 "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $180,600 ", 
           "Roof Top Cell Site":" $319,200 ", 
           "Cell Site On Structure":" $319,200 "
        },
        "Fiber":{
           "Fiber Terminal":" $12,000 ", 
           "Fiber Hut":" $33,000 "
        },
        "Indoor Equipment":{
           "Mini Data Center":"$84,000", 
           "Dedicated Room":"$180,000"
        }
    },
    "Rural":{
        "Small Cell Site":{
           "Indoor Small Cell":" $33,600 ", 
           "Outdoor Small Cell":" $42,000 ", 
           "Small Cell On New Pole":" $50,400 ", 
           "Small Cell On Structure":" 58,800 "
        },
        "Full Size Cell Site":{
           "Cell Tower":" $138,600", 
           "Roof Top Cell Site":" $252,000", 
           "Cell Site On Structure":" $285,600 "
        },
        "Fiber":{
           "Fiber Terminal":" $9,000 ", 
           "Fiber Hut":" $27,000"
        },
        "Indoor Equipment":{
           "Mini Data Center":"$54,000", 
           "Dedicated Room":"$120,000"
        }
    }
   }
}  

// When the user clicks on the button, open the modal
btn.onclick = function() {
   modal.style.display = "block";
 }
 



  
  
  function closeForm() {
    document.getElementById("myForm").style.display = "none";
  }
/*Local Storage*/
var id=new Array();
var value=new Array();
var name;
var lname;
var mail;

$('#save').on('click', function set(){
  


var name = document.getElementById("Firstname").value;
var lname = document.getElementById("LastName").value;
var mail = document.getElementById("Email1").value;

//Store it in the localStorage
localStorage.setItem("First Name", name);
localStorage.setItem("Last Name", lname);
localStorage.setItem("Email", mail);
});
function SwapDivsWithClick(mcq1,mcq2)
{
   d1 = document.getElementById(mcq1);
   d2 = document.getElementById(mcq2);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }
}
function SwapDivsWithClick1(mcq2,mcq3)
{
   d1 = document.getElementById(mcq2);
   d2 = document.getElementById(mcq3);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }
}


function SwapDivsWithClick3(mcq4,myForm)
{
   d1 = document.getElementById(mcq4);
   d2 = document.getElementById(myForm);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }
}
function SwapDivsWithClick4(myForm,right)
{
   d1 = document.getElementById(myForm);
   d2 = document.getElementById(right);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }
   var img = document.createElement("img");
 
  img.src = "mo.png";
  var src = document.getElementById("resimg");
  
  src.appendChild(img);
  var para = document.createElement("P");                 // Create a <p> element
para.innerHTML = " Expected Rent: $650 Per Month";                // Insert text
document.getElementById("res").appendChild(para); 

   result();
}
function goToForm(right,mcq1)
{
   d1 = document.getElementById(right);
   d2 = document.getElementById(mcq1);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }
}

function mcq1(id1)
{
   val1=document.getElementById(id1).value ;
}
function mcq2(id2)
{
   val2=document.getElementById(id2).value ;
}
function mcq3(id3)
{
   val3=document.getElementById(id3).value ;
}
function mcq4(id4)
{
   val4=document.getElementById(id4).value ;
}
function result()
{
var para5 = document.createElement("P");                 // Create a <p> element
para5.innerHTML = " Expected Rent: " + rent[val1][val2][val3][val4] + " Per Month";    
   
document.getElementById("res").appendChild(para5);
var para6 = document.createElement("P");                 // Create a <p> element
para6.innerHTML = " Current Value of Telecom Lease : " + current[val1][val2][val3][val4];    
   
document.getElementById("res").appendChild(para6);
}

//checking login required
function NoLogin(mcq4,right)
{
   d1 = document.getElementById(mcq4);
   d2 = document.getElementById(right);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }


}
function check(mcq4,myForm,right)
{
if(localStorage.getItem("Email")!=null)
{
   NoLogin(mcq4,right);
   var src = document.getElementById("resimg");
   var img = document.createElement("img");
   img.src = "mo.png";
  
   src.appendChild(img);
  
   result();
}
else
{
   SwapDivsWithClick3(mcq4,myForm);
}

}
function clearBox(el1,el2)
{
    document.getElementById(el1).innerHTML = "";
    document.getElementById(el2).innerHTML = "";
}
 function goback1(mcq2,mcq1)
 {
   d1 = document.getElementById(mcq2);
   d2 = document.getElementById(mcq1);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }
 }
 function goback2(mcq3,mcq2)
 {
   d1 = document.getElementById(mcq3);
   d2 = document.getElementById(mcq2);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }
 }
 function goback3(mcq4,mcq3)
 {
   d1 = document.getElementById(mcq4);
   d2 = document.getElementById(mcq3);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }
 }
 function goback4(myForm,mcq4)
 {
   d1 = document.getElementById(myForm);
   d2 = document.getElementById(mcq4);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }
 }

  function SwapDivsC1(mcq3,mcqC1)
 {
   d1 = document.getElementById(mcq3);
   d2 = document.getElementById(mcqC1);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }
 } 
 function SwapDivsC2(mcq3,mcqC2)
 {
   d1 = document.getElementById(mcq3);
   d2 = document.getElementById(mcqC2);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }
 } 
 function SwapDivsC3(mcq3,mcqC3)
 {
   d1 = document.getElementById(mcq3);
   d2 = document.getElementById(mcqC3);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }
 } 
 function SwapDivsC4(mcq3,mcqC4)
 {
   d1 = document.getElementById(mcq3);
   d2 = document.getElementById(mcqC4);
   if( d2.style.display == "none" )
   {
      d1.style.display = "none";
      d2.style.display = "block";
   }
   else
   {
      d1.style.display = "block";
      d2.style.display = "none";
   }
 } 
 $("#SmallCellSite").click(function(){
    $("#decide").click(function(){  
       goback4('myForm','mcqC1');
    });
});
$("#FullCellSite").click(function(){
    $("#decide").click(function(){  
       goback4('myForm','mcqC2');
    });
});
$("#Fiber").click(function(){
    $("#decide").click(function(){  
       goback4('myForm','mcqC3');
    });
});
$("#IndoorEquipment").click(function(){
    $("#decide").click(function(){  
       goback4('myForm','mcqC4');
    });
});