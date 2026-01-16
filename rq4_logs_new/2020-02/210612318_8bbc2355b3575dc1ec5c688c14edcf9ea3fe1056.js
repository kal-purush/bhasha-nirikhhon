import React from "react"
import { Link } from "gatsby"

import Layout from "../components/layout"
import {PageHeader} from "../components/page-header"
import SEO from "../components/seo"

import "./styles/index.scss"

const FactsPage = () => (
  <Layout>
    <div className="page-container">
        <PageHeader pageTitle="Pacifier Facts and Recommendations"/>
        <div className="page facts-page">
            <p>Oralign Baby bases all of our claims and recommendations on peer-reviewed scientific literature. With that said, pacifiers cannot help a child’s teeth or jaws to grow straight or normal. This includes the Classic Plus and all of our future pacifiers. All pacifiers are foreign objects that produce forces in the mouth that would not be there otherwise. All that we can do is to lower and/or change the direction of those forces so as to lower the chance of, or the degree of unwanted oral structural changes. All pacifiers collapse to take the shape of a child’s palate. Traditional and orthodontic (physiological) pacifiers can both cause orthodontic problems. The literature shows that both types of pacifiers clinically show the same problems, with orthodontic versions possibly showing more unwanted orthodontic changes in the back of the mouth and more horizontal problems in the front of the mouth. The traditional versions show possibly more of a vertical problem in the front of the mouth. That is the reason for the development of the pressure reducing Comfort Stem, which lowers pressure by reducing the influence of the shield by more than 40% with its proprietary inverted stem to base angulation.</p>
            <p>As a clinician with thirty years of experience, I strongly recommend the use of a pacifier from the age of one month to one year. Pacifier use, especially during sleep, reduces the chance of your child experiencing Sudden Infant Death Syndrome by as much as 95%.</p>
            <p>I also strongly recommend to discontinue pacifier use once your child has reached the age of one year. Yes, I want to sell you less pacifiers, as at the very least there is no health benefit to pacifier use past the age of one year. A 2012 University of Oklahoma study by Dr. Thomas Glass, found that after two weeks of pacifier use, a bacterial biofilm was formed on pacifiers, teethers, and baby bottle nipples that could not be removed by any method used at home such as boiling, or cleaning with any antimicrobial agent. His recommendation is to replace a pacifier every two weeks. It is interesting that over the years I have seen a higher incidence of otitis media in pacifier users. I also question the red rash that sometimes is seen around the mouth in pacifier users, may in some children actually be a skin infection from an infected pacifier.</p>
            <p>I strongly recommend boiling or steam via a microwave to “sterilize” a pacifier before use once cooled. At the very least pacifiers need to be thoroughly cleaned with soap and water or soaked in vinegar.</p>
            <p>I hope that these facts help you in your decision process for proper pacifier use for your child.</p>
            <p>– Dr. John J. Davis CEO</p>
            <div className="fact-links">
                <h1>Read The Facts</h1>
                <div>
                    <h2>Are Pacifiers Safe? Pacifier Pros, Cons & Safety Considerations</h2>
                    <a target="_blank" href="https://momlovesbest.com/pacifier-safety-guide ">https://momlovesbest.com/pacifier-safety-guide</a>
                </div>
                <div>
                    <h2>Pacifier use reduces the risk of Sudden Infant Death Syndrome by up to 95%.</h2>
                    <a target="_blank" href="https://pediatrics.aappublications.org/content/116/5/e716">https://pediatrics.aappublications.org/content/116/5/e716</a>
                </div>
                <div>
                    <h2>All pacifier types can cause orthodontic changes in a child’s mouth.</h2>
                    <a target="_blank" href="http://www.aapd.org/assets/1/25/Zardetto6-02.pdf">http://www.aapd.org/assets/1/25/Zardetto6-02.pdf</a>
                    <a target="_blank" href="http://www.aapd.org/assets/1/25/Adair-14-01.pdf">http://www.aapd.org/assets/1/25/Adair-14-01.pdf</a>

                </div>
                <div>
                    <h2>Thumbs can become stuck and loose circulation with the uni-mold - single piece type of pacifiers.</h2>
                    <a target="_blank" href="https://www.saferproducts.gov/Search/Result.aspx?dm=0&q=pacifier+thumb+stuck&srt=0">https://www.saferproducts.gov/Search/Result.aspx?dm=0&q=pacifier+thumb+stuck&srt=0</a>
                    <a target="_blank" href="https://forums.thebump.com/discussion/12173686/soothie-warning">https://forums.thebump.com/discussion/12173686/soothie-warning</a>
                    <a target="_blank" href="https://www.whattoexpect.com/forums/december-2013-babies/topic/soothie-pacifierswarning.html">https://www.whattoexpect.com/forums/december-2013-babies/topic/soothie-pacifierswarning.html</a>
                    <a target="_blank" href="https://www.usa.philips.com/c-p/SCF190_02/avent-soothie-pacifier/reviewandawards">https://www.usa.philips.com/c-p/SCF190_02/avent-soothie-pacifier/reviewandawards</a>
                    <a target="_blank" href="https://www.amazon.com/product-reviews/B0045I6IAY/ref=cm_cr_othr_d_paging_btm_4? ie=UTF8&filterByStar=one_star&showViewpoints=0&pageNumber=4">https://www.amazon.com/product-reviews/B0045I6IAY/ref=cm_cr_othr_d_paging_btm_4? ie=UTF8&filterByStar=one_star&showViewpoints=0&pageNumber=4</a>
                    <a target="_blank" href=""></a>
                </div>
                <div>
                    <h2>Excellent European study on finger sizes and entrapment opening measurements. The internal diameter of a traditional round pacifier is 8.0 - 8.2mm. The mean average diameter of a 0 - 2M old baby is 8.6mm. By age one the average thumb diameter is 11mm.</h2>
                    <a target="_blank" href="http://www.anec.eu/attachments/ANEC-R%26T-2012-CHILD-001final.pdf">http://www.anec.eu/attachments/ANEC-R%26T-2012-CHILD-001final.pdf</a>

                </div>
                <div>
                    <h2>One of many links reporting on the University of Oklahoma study on contaminated pacifiers.</h2>
                    <a target="_blank" href="http://healthland.time.com/2012/11/15/bacteria-on-binkies-a-recipe-for-crankiness/">http://healthland.time.com/2012/11/15/bacteria-on-binkies-a-recipe-for-crankiness/</a>
                </div>
            </div>
        </div>
    </div>
  </Layout>
)

export default FactsPage