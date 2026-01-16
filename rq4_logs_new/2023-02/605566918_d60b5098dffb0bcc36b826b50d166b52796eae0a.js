import './footer.css'
import twitter from '../../assets/images/twitter.png'
import yt from '../../assets/images/youtube.png'
import web from '../../assets/images/web-link.png'
import linkedin from '../../assets/images/linkedin.png'
import kaizen from '../../assets/images/kaizen.png'
import insta from '../../assets/images/instagram.png'
import fb from '../../assets/images/facebook.png'

export default function Footer(){
    return(
        <footer id="footer" class="footer">
        <div class="footer__kaizen">
            <div class="kaizen-title">
            <h2>Kaizen</h2>
            <img src={kaizen} alt=""/>
            </div>
            <p class="kaizen-desc">
            Lorem ipsum dolor sit amet consectetur adipisicing elit. Mollitia facilis consectetur temporibus asperiores
            eius quae dolor beatae non dolorum. Mollitia quas est obcaecati, impedit earum cum! Recusandae, provident
            quos? Eveniet?
            </p>

        </div>
        <div class="footer__reach">
            <div class="reach-contact">
            <h2>CONTACT US</h2>
            <hr/>
            <ul>
                <li>
                <span>Phone :</span>
                +91 1234567890
                </li>
                <li>
                <span>Email :</span>
                nssiitd.team@abc.in
                </li>
            </ul>
            </div>
            <div class="reach-socials">
            <a href="https://www.facebook.com/NSSIITDelhi" target="blank">
                <img alt="FB" src={fb} />
            </a>
            <a href="https://www.linkedin.com/company/nss-iit-delhi/?originalSubdomain=in" target="blank">
                <img alt="LIN" src={linkedin} />
            </a>
            <a href="https://www.instagram.com/nssiitd/?hl=en" target="blank">
                <img alt="INST" src={insta} />
            </a>
            <a href="https://www.youtube.com/channel/UC3nq2Tsw8eruu22-MBr2_ow" target="blank">
                <img alt="yt" src={yt} />
            </a>
            <a href="https://mobile.twitter.com/nss_iitd" target="blank">
                <img alt="TWITTER" src={twitter} />
            </a>
            <a href="http://nss.iitd.ac.in/#!/" target="blank">
                <img alt="FB" src={web}/>
            </a>
            </div>
        </div>

        <section class="footer__copyright">© 2023 NSS IIT DELHI - All rights reserved <br/>
            <span>
            Designed by NSS Tech Team
            </span>
        </section>
        </footer>
    );
}