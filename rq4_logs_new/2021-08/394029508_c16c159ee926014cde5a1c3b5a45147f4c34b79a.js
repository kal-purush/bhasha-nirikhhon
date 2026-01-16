import React from 'react';
import InfoIcon from '@material-ui/icons/Info';
import FiberManualRecordIcon from '@material-ui/icons/FiberManualRecord';

const Widgets = () => {
    const newsArticle = (heading, subtitle) => (
        <div className="widgets__article">
            <div className="widgets__articleLeft">
                <FiberManualRecordIcon />
            </div>

            <div className="widgets__articleRight">
                <h4>{heading}</h4>
                <p>{subtitle}</p>
            </div>
        </div>
    )

    return (
        <div className="widgets">
            <div className="widgets__header">
                <h2>LinkedIn News</h2>
                <InfoIcon />
            </div>

            {newsArticle("React est le futur ! " , "Top news - 9999 lecteurs")}
            {newsArticle("Changeons le monde ! " , "Top news - 9999 lecteurs")}
            {newsArticle("L'avenir c'est d'être freelance " , "Top news - 9999 lecteurs")}
            {newsArticle("Apprendre de ses erreurs" , "Top news - 9999 lecteurs")}
            {newsArticle("Réussir sa vie" , "Top news - 9999 lecteurs")}
            {newsArticle("Toujours allez de l'avant" , "Top news - 9999 lecteurs")}
        </div>
    );
};

export default Widgets;