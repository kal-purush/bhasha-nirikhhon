var dictionary={
"Total population within the locality": ["B01003001"],
"Age distribution broken down by sex": ["B01001001"],
"Median age by sex": ["B01002001"],
"Race": ["B02001001"],
"Living arrangement for adults (18 years and over)": ["B09021001"],
"Place of birth by nativity": ["C05002001"],
"Median household income": ["B19013001"],
"Per capita income": ["B19301001"],
"Income to poverty-level ratio": ["B17002001"],
"Poverty level by place of birth": ["B06012001"],
"Educational attainment by place of birth": ["B06009001"],
"Travel time to work": ["B08303001"],
"Means of transportation to work": ["B08301001"]
};

function loadtreemap(dataselection){
    if("B01003_001E" == dataselection){

    }
    if("B01001_001E" == dataselection){
        var str ="  <label class='btn btn-default' style='width:50%;height:100%;white-space:normal;font-size:11px;border-radius: 2px;'><input type='radio' class='toggle' name='dataset' value='"+"B01001_002E"+"'>"+"Male"+"</label>";
        str = str+ "<label class='btn btn-default' style='width:50%;height:100%;white-space:normal;font-size:11px;border-radius: 2px;'><input type='radio' class='toggle' name='dataset' value='"+"B01001_026E"+"'>"+"Female"+"</label>";
        $("#level2").html( str);

    }
    if("B01002_001E" == dataselection){
        var str ="  <label class='btn btn-default' style='width:50%;height:100%;white-space:normal;font-size:11px;border-radius: 2px;'><input type='radio' class='toggle' name='dataset' value='"+"B01002_002E"+"'>"+"Male"+"</label>";
        str = str+ "<label class='btn btn-default' style='width:50%;height:100%;white-space:normal;font-size:11px;border-radius: 2px;'><input type='radio' class='toggle' name='dataset' value='"+"B01002_003E"+"'>"+"Female"+"</label>";
        $("#level2").html( str);
    }
    if("B02001_001E" == dataselection){
        var str ="  <label class='btn btn-default' style='width:30%;height:33.33%;white-space:normal;font-size:11px;border-radius: 2px;'><input type='radio' class='toggle' name='dataset' value='"+"B01002_002E"+"'>"+"White Alone"+"</label>";
        str = str+ "<label class='btn btn-default' style='width:30%;height:33.33%;white-space:normal;font-size:11px;border-radius: 2px;'><input type='radio' class='toggle' name='dataset' value='"+"B01002_003E"+"'>"+"Asian Alone"+"</label>";
        str = str+ "<label class='btn btn-default' style='width:40%;height:33.33%;white-space:normal;font-size:11px;border-radius: 2px;'><input type='radio' class='toggle' name='dataset' value='"+"B01002_003E"+"'>"+"Some Other Race Alone"+"</label>";
        str = str+ "<label class='btn btn-default' style='width:40%;height:33.33%;white-space:normal;font-size:11px;border-radius: 2px;'><input type='radio' class='toggle' name='dataset' value='"+"B01002_003E"+"'>"+"Black or African American Alone"+"</label>";
        str = str+ "<label class='btn btn-default' style='width:60%;height:33.33%;white-space:normal;font-size:11px;border-radius: 2px;'><input type='radio' class='toggle' name='dataset' value='"+"B01002_003E"+"'>"+"Native Hawaiian and Other Pacific Islander Alone"+"</label>";
        str = str+ "<label class='btn btn-default' style='width:60%;height:33.33%;white-space:normal;font-size:11px;border-radius: 2px;'><input type='radio' class='toggle' name='dataset' value='"+"B01002_003E"+"'>"+"American Indian and Alaska Native Alone"+"</label>";
        str = str+ "<label class='btn btn-default' style='width:40%;height:33.33%;white-space:normal;font-size:11px;border-radius: 2px;'><input type='radio' class='toggle' name='dataset' value='"+"B01002_003E"+"'>"+"Two or More Races"+"</label>";
        $("#level2").html( str);
    }
    if("B09021_001E" == dataselection){

    }
    if("C05002_001E" == dataselection){

    }
    if("B19013_001E" == dataselection){

    }
    if("B19301_001E" == dataselection){

    }
    if("B17002_001E" == dataselection){

    }
    if("B06012_001E" == dataselection){

    }
    if("B06009_001E" == dataselection){

    }
    if("B08303_001E" == dataselection){

    }
    if("B08301_001E" == dataselection){

    }
}