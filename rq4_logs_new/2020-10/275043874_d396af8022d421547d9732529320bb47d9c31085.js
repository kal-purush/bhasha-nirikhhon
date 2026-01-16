import React from "react"

import { Col, Row } from "react-bootstrap"
import Layout from "../components/layout"
import SEO from "../components/seo"
import { pages } from "../consts"
import { Link } from "gatsby"


const Analytics = ({ location }) => {


    const { keywords, image, description } = pages.ANALYTICS.meta
    const { name } = pages.ANALYTICS

    return (
        <Layout id="analytics-page">
        <SEO
            url={location.href}
            title={name}
            keywords={keywords}
            image={image}
            description={description}
        />
        <Row className="ancillary-page-heading">
            <Col
            className=""
            xs={{ offset: 1, span: 10 }}
            md={{ offset: 1, span: 8 }}
            lg={{ offset: 1, span: 8 }}
            xl={{ offset: 2, span: 8 }}
            >
            <div className="content">
                <div className="page-title-section">
                    <h2 className="subtitle">Get the data</h2>
                   
                </div>
            </div>
            </Col>
        </Row>
        <Row noGutters>
            <Col
            className="text-blocks"
            xs={{ offset: 1, span: 10 }}
            md={{ offset: 1, span: 8 }}
            lg={{ offset: 1, span: 7 }}
            xl={{ offset: 2, span: 7 }}
            >
            
            </Col>
        </Row>
        </Layout>  
    )
}    

export default Analytics