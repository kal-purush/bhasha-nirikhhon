import Navbar from "../components/Navbar"
import Hero from "../components/Hero";
import AboutImg from "../assets/3.jpg";
import Footer from "../components/Footer";
import AboutUs from "../components/AboutUs";

function About (){
    return(
        <>
        <Navbar/>
        
        <Hero
       cName="hero-mid"
       heroImg={AboutImg}
       title="About Us"
       btnClass="hide"
       />
        <AboutUs/>
        <Footer/>
        </>
    )
    }
    
    export default About;