import React from 'react';

import {  Card, CardImg, CardBody, CardTitle,CardText, CardSubtitle} from 'reactstrap';

const AboutUS = () =>
{
    return(
        <div className="container">
            <div className="row row-main">

            <div className="col-12 about-box">
                <div className="about-heading">
                        Our Faculties
                </div>
                <div className="row justify-content-around">
                <Card className="col-12 col-lg-5 staff-intro">
                    <CardImg className="staff-photo" top width="100%" src="./images/gct_principal.jpg" alt="Card image cap" />
                    <CardBody>
                        <CardTitle tag="h5">Dr.P.Thamarai,Ph.D.</CardTitle>
                        <CardSubtitle tag="h6" className="mb-2 text-muted">Principal</CardSubtitle>
                        <CardText>Government College of Technology</CardText>
                    </CardBody>
                </Card>
                <Card className="col-12 col-lg-5 staff-intro">
                    <CardImg className="staff-photo" top width="100%" src="./images/tmfa_advisor.jpg" alt="Card image cap" />
                    <CardBody>
                        <CardTitle tag="h5">Dr.J.Anbazhagan Vijay.</CardTitle>
                        <CardSubtitle tag="h6" className="mb-2 text-muted">TMFA- Advisor</CardSubtitle>
                    </CardBody>
                </Card>
                </div>
            </div>

                <div className="col-12 about-box">
                    <div className="about-heading">
                        Tamil Mandram and Fine Arts club of GCT - TMFA
                    </div>
                    <img className="about-photo" href={`#k`} src="./images/gct.jpg" alt="hacker" />
                    <div className="about-details">
                    Tamil Mandram and Fine Arts club of Government College of Technology actively involve in many activities to develop and create awareness about tamil arts and literature among students. In 1975 october 1, Tamil Mandram and Fine Arts club was started. Since then it has done many activities such as cultural fests, research activities, and social activities. There are more than 10 group events under Tamil Arts viz Villupattu, Streetplay, Mime, Drama, Puppet show, Modern drama, Silambattam, Paraiattam, Folk dance and lights show We have been learning and also participating in many college events. 23 books were published by our club students. . Now in this lockdown we have initiated this hackathon to create awareness among students about the importance of tamil computation and  technology in language.
                    </div>
                </div>
                <div className="col-12 about-box">
                    <div className="about-heading">
                        International Forum for Information Technology For Tamil - INFITT
                    </div>
                    <div className="about-details">
                    Tamil Mandram and Fine Arts club of Government College of Technology actively involve in many activities to develop and create awareness about tamil arts and literature among students. In 1975 october 1, Tamil Mandram and Fine Arts club was started. Since then it has done many activities such as cultural fests, research activities, and social activities. There are more than 10 group events under Tamil Arts viz Villupattu, Streetplay, Mime, Drama, Puppet show, Modern drama, Silambattam, Paraiattam, Folk dance and lights show We have been learning and also participating in many college events. 23 books were published by our club students. . Now in this lockdown we have initiated this hackathon to create awareness among students about the importance of tamil computation and  technology in language.
                    </div>
                </div>
            </div>
        </div>
    );
}

export default AboutUS;