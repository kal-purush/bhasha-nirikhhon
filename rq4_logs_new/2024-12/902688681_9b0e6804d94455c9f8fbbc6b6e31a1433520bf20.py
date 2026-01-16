import pytest 
from datetime import datetime as dt 
import pandas as pd 
import sys
import os

#print("Current Path:", os.getcwd())
#print("Adding Path:", os.path.abspath(os.path.join(os.path.dirname(__file__), "../code")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../code")))
from functions import *


# Test for get_lat_lon 
def test_get_lat_lon(): 
    lat, lon = get_lat_lon("Paris") 
    assert lat is not None and lon is not None 
    assert isinstance(lat, float) and isinstance(lon, float) 

# Test for reverse_geocode 
def test_reverse_geocode(): 
    lat, lon = 48.8566, 2.3522 
    city, nation = reverse_geocode(lat, lon) 
    assert city == "Paris" 
    assert nation == "France" 

 

# Test for get_timezone 
def test_get_timezone(): 
    lat, lon = 48.8566, 2.3522 
    timezone = get_timezone(lat, lon) 
    assert timezone is not None 
    assert isinstance(timezone, str) 

 

# Test for get_birth_chart_data 
def test_get_birth_chart_data(): 
    name = "Sofia" 
    date = dt.strptime("2004-09-19", "%Y-%m-%d").date() 
    time = dt.strptime("11:11", "%H:%M").time() 
    lat, lon = 48.8566, 2.3522 
    city, nation = "Paris", "France" 
    timezone = "CET" 
    data = get_birth_chart_data(name, date, time, lat, lon, city, nation, timezone) 
    assert data is not None 
    assert 'data' in data 

# Test for process_birth_chart 

def test_process_birth_chart(): 
    raw_data = { 
        "data": { 
            "sun": {"name": "Sun", "sign": "Vir", "position": "123"}, 
            "moon": {"name": "Moon", "sign": "Sag", "position": "456"} 
       } 
    } 
    df = process_birth_chart(raw_data) 
    assert isinstance(df, pd.DataFrame) 
    assert "sign" in df.columns 
    assert df.loc["sun", "sign"] == "Virgo" 

# Test for get_big_three 

def test_get_big_three(): 
    raw_data = { 
        "data": { 
            "first_house": {"sign": "Gem"} 
        } 
    } 
    chart = pd.DataFrame({ 
        "name": ["Sun", "Moon"], 
        "sign": ["Leo", "Sagittarius"] 
    }) 
    big_three = get_big_three(raw_data, chart) 
    assert big_three["sun"] == "Leo" 
    assert big_three["moon"] == "Sagittarius" 
    assert big_three["rising"] == "Gemini" 

# Test for get_horoscope_data 

def test_get_horoscope_data(): 
    sign = "Virgo" 
    result = get_horoscope_data(sign) 
    assert result is not None 
    assert isinstance(result, str) 

# Test for get_more_info 

def test_get_more_info(): 
    placement, sign = "Sun", "Virgo" 
    placement_info, zodiac_info = get_more_info(placement, sign) 
    assert "core identity" in placement_info 
    assert "detail-oriented" in zodiac_info 

# Test for plot_scatter 

def test_plot_scatter(): 
    df = pd.DataFrame({ 
        "name": ["Sun", "Moon"], 
        "sign": ["Leo", "Sagittarius"], 
        "element": ["Fire", "Fire"], 
        "position": ["10", "20"] 
    }) 
    fig = plot_scatter(df) 
    assert fig is not None 

 

# Test for plot_pie_chart 
def test_plot_pie_chart(): 
    df = pd.DataFrame({ 
        "name": ["Sun", "Moon"], 
        "sign": ["Leo", "Sagittarius"], 
        "element": ["Fire", "Fire"] 
    }) 
    fig = plot_pie_chart(df) 
    assert fig is not None 