import React, { useState, useEffect } from "react";
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faUpLong } from '@fortawesome/free-solid-svg-icons'

import classes from './Filter.module.css'
const Filter = (props) => {
    const [filters, setFilters] = useState({
        EMA10: "",
        EMA21: "",
        EMA50: "",
        EMA200: "",
        VolumeDays: 20,
        VolumeAvg: 50000,
        Price: 5000
    });

    useEffect(() => {
        var query = buildSearchQuery();
        console.log(query);
        props.onFilterChange(query);

    }, []);

    // function to handle changes in dropdowns
    const handleFilterChange = (event) => {
        console.log(event.target.value);
        const { name, value } = event.target;

        setFilters((prevFilters) => ({ ...prevFilters, [name]: value }));
    };

    const handleSearchSubmit = (event) => {
        event.preventDefault();
        var query = buildSearchQuery();
        console.log(query);
        props.onFilterChange(query);
    };

    const buildSearchQuery = () => {
        var query = "";

        if (filters.EMA10 !== "") {
            query = filters.EMA10;
        }

        if (filters.EMA21 !== "") {
            if (query.length > 0) {
                query = query + "&";
            }
            query = query + filters.EMA21;
        }
        if (filters.EMA50 !== "") {
            if (query.length > 0) {
                query = query + "&";
            }
            query = query + filters.EMA50;
        }

        if (filters.EMA200 !== "") {
            if (query.length > 0) {
                query = query + "&";
            }
            query = query + filters.EMA200;
        }

        if ((+filters.VolumeDays) !== 0 && (+filters.VolumeAvg) !== 0) {
            console.log(filters.VolumeDays, filters.VolumeAvg);
            if (query.length > 0) {
                query = query + "&";
            }

            query = query + "volavg=" + filters.VolumeDays + "_" + filters.VolumeAvg;
        }
        if ((+filters.Price) !== 0) {
            if (query.length > 0) {
                query = query + "&";
            }

            query = query + "pr=" + filters.Price;
        }
        return query;
    }
    return (
        <div className={classes.filterContainer}>
            <form onSubmit={handleSearchSubmit} className="">
                <div className="form-row">
                    <div className="form-group col-sm-2">
                        <label htmlFor="filterEma10">
                            EMA 10
                        </label>
                        <select name="EMA10" id="filterEma10" className="form-control" value={filters.category} onChange={handleFilterChange}>
                            <option value=""></option>
                            <option value="ema10=1">above &#xf062;</option>
                            <option value="ema10=2">below &#xf063;</option>
                        </select>
                    </div>
                    <div className="form-group col-sm-2">
                        <label htmlFor="filterEma21">
                            EMA 21
                        </label>
                        <select name="EMA21" id="filterEma21" className="form-control" value={filters.priceRange} onChange={handleFilterChange}>
                            <option value=""></option>
                            <option value="ema21=1">above &#xf062;</option>
                            <option value="ema21=2">below &#xf063;</option>
                        </select>
                    </div>
                    <div className="form-group col-sm-2">
                        <label htmlFor="filterEma50">
                            EMA 50
                        </label>
                        <select name="EMA50" id="filterEma50" className="form-control" value={filters.EMA50} onChange={handleFilterChange}>
                            <option value=""></option>
                            <option value="ema50=1">above &#xf062;</option>
                            <option value="ema50=2">below &#xf063;</option>
                        </select>
                    </div>
                    <div className="form-group col-sm-2">
                        <label htmlFor="filterEma200">
                            EMA 200
                        </label>
                        <select name="EMA200" id="filterEma200" className="form-control" value={filters.EMA200} onChange={handleFilterChange}>
                            <option value=""></option>
                            <option value="ema200=1">above &#xf062;</option>
                            <option value="ema200=2">below &#xf063;</option>
                        </select>
                    </div>
                    <div className="form-group col-sm-2">
                        <label htmlFor="filterPrice" >
                            {'Price'}
                        </label>
                        <FontAwesomeIcon icon={faUpLong} />

                        <input name="Price" id="filterPrice" className="form-control" type="number" value={filters.Price} onChange={handleFilterChange} ></input>
                    </div>
                    <div className="form-row col-sm-4">
                        <div className="form-group col-sm-6">
                            <label htmlFor="filterVolumeDays" >
                                Average volume :
                            </label>
                            <input name="VolumeDays" id="filterVolumeDays" className="form-control" type="number" value={filters.VolumeDays} onChange={handleFilterChange} ></input>
                        </div>
                        <div className="form-group col-sm-6">
                            <label htmlFor="filterVolumeAvg">
                                <FontAwesomeIcon icon={faUpLong} />
                            </label>
                            <input name="VolumeAvg" id="filterVolumeAvg" className="form-control" type="number" value={filters.VolumeAvg} onChange={handleFilterChange}></input>
                        </div>
                    </div>
                </div>
                <button type="submit" className="btn btn-primary">Search</button>
            </form>
        </div>
    );
};

export default Filter;