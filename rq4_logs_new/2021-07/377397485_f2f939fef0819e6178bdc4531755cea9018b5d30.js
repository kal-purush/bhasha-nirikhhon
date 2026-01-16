import React from 'react';

//components
import Nav from './Nav';
import Footer from './Footer';

export default function Shop() {
    return (
        <div>
            <Nav />
            <div className="shop">
                <div className="shop-selection">
                    <div className="shop-category">
                        <h4>Category</h4>
                        <label><input type="radio" name="category" value="all" defaultChecked />All</label>
                        <label><input type="radio" name="category" value="racquet" />Racquets</label>
                        <label><input type="radio" name="category" value="apparel" />Apparel</label>
                        <label><input type="radio" name="category" value="ball" />Tennis Balls</label>
                    </div>
                    <div className="shop-category">
                        <h4>Category</h4>
                        <label><input type="radio" name="category" value="wilson" />Wilson</label>
                        <label><input type="radio" name="category" value="babolat" />Babolat</label>
                        <label><input type="radio" name="category" value="yonex" />Yonex</label>
                    </div>
                </div>
            </div>
            <Footer />
        </div>
    )
}