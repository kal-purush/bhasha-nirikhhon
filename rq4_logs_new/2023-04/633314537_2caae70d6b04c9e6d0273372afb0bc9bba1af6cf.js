import React, { Component } from "react";
import { Helmet } from "react-helmet";
import { Image } from "react-bootstrap";
import Screen3 from "../../assets/Low-Power-Displays-Main-Image--F.jpg"
import Screen4 from "../../assets/Low-Power-Displays-Image-2.jpg"
import logo from "../../assets/sharp_logo.png";
import arrow from "../../assets/arrow.png";
import temperature from "../../assets/Wide Temperatures Icon.png"
import thinLightweight from "../../assets/Thin and Lightweight Icon.png"
import outdoor from "../../assets/Outdoor Readability Icon.png"
import lowPower from "../../assets/Low-Power Icon.png"
import Button from "react-bootstrap/Button";
import Footer from "../../Footer";
import axios from "axios";
import Toast from "react-bootstrap/Toast";
import "react-loader-spinner/dist/loader/css/react-spinner-loader.css";
import Loader from "react-loader-spinner";
import Form1 from "react-bootstrap/Form";
import Col from "react-bootstrap/Col";
import image2 from "../../assets/Memory in Pixel LCD.png"
import image4 from "../../assets/R-IGZO LCD.png"

export default class LowPowerDisplay extends Component {
    constructor(props) {
        super(props);
        this.state = {
            name: null,
            mobileCode: "",
            mobile: null,
            email: null,
            message: null,
            show: false,
            showLoader: false,
            submit: false,
            showError: false,
            demo: false,
        };
        this.myRef = React.createRef();
    }
    callFunct = (event, link) => {
        event.preventDefault();
        window.location.href = "#" + link;
        // history.push(link);
        // alert(text);
    };
    onSubmit = (event) => {
        event.preventDefault();
        this.setState({
            showLoader: true,
            show: false,
        });
        let data = {
            name: this.state.name,
            mobile: this.state.mobileCode + "-" + this.state.mobile,
            email: this.state.email,
            message: this.state.demo
                ? "Yes, I’d like to request a demo. " + this.state.message
                : this.state.message,
            subject: ["5-inch R-IGZO Landing"],
        };
        axios
            .post("/api/form", data)
            .then((res) => {
                this.setState({ showLoader: false, show: true });
                this.setState({
                    name: "",
                    mobileCode: "",
                    mobile: "",
                    email: "",
                    message: "",
                    submit: true,
                    demo: false,
                });
            })
            .catch((error) => {
                console.log(error);
                this.setState({
                    showLoader: false,
                    show: false,
                    showError: true,
                    submit: false,
                });
            });
    };
    callFunct = (event, link) => {
        event.preventDefault();
        window.location.href = "#" + link;
        // alert(text);
    };
    componentDidMount() {
        window.scrollTo({ top: 0, behavior: "auto" });
    }
    render() {
        return (
            <div>
                <Helmet>
                    <title>Memory-in-Pixel - Sharp</title>
                    <meta name="description" content="Sharp-Memory-in-Pixel" />
                </Helmet>

                <div
                    style={{
                        alignItems: "center",
                        backgroundImage: `url(${Screen3})`,
                        backgroundSize: "cover",
                        backgroundPosition: "center center",
                        backgroundRepeat: "no-repeat",
                        minHeight: "90vh",
                    }}
                >
                    <div
                        style={{
                            alignItems: "center",
                            justifyContent: "center",
                            display: "flex",
                            flexDirection: "column",
                            padding: 20,
                        }}
                    >
                        <Image
                            role="presentation"
                            alt="SHARP Logo"
                            src={logo}
                            style={{ width: "35%", paddingTop: "50px", maxWidth: 300 }}
                        />
                        <div style={{ paddingTop: 150 }}>
                            <h1
                                style={{
                                    color: "white",
                                    fontSize: "3.5rem",
                                    textAlign: "center",
                                    fontWeight: 'bold'
                                }}
                            >
                                YOUR SOURCE FOR GAME CHANGING LOW POWER DISPLAYS
                            </h1>
                        </div>
                        <Image
                            role="presentation"
                            alt="arrow"
                            src={arrow}
                            style={{ maxWidth: "32px", paddingTop: 80 }}
                        />
                    </div>
                </div>
                <div
                    style={{
                        display: "flex",
                        alignItems: "center",
                        flexDirection: "column",
                    }}
                >

                    <div
                        style={{
                            width: "100%",
                            justifyContent: "center",
                            display: "flex",
                        }}
                    >
                        <div
                            className="row"
                            style={{
                                maxWidth: "100%",
                                display: "flex",
                                alignItems: "center",
                                paddingBlock: 20,
                                paddingInline: '10%',
                                margin: 0,
                            }}
                        >
                            <div>
                                <div className="padding-top-50 d-none d-md-block"></div>
                                <div className="padding-top-50"></div>
                                <h1 style={{ fontSize: "2.5rem", textAlign: 'center', border: 'none', marginInline: 'auto', width: '80%' }}>
                                    <strong>PUT THE MUSCLE OF SHARP LOW-POWER
                                        DISPLAYS TO WORK IN YOUR NEXT DESIGN</strong>
                                </h1>
                                <div className="padding-top-50"></div>
                            </div>
                            <p style={{
                                fontWeight: 'light',
                                lineHeight: '2.5rem',
                                letterSpacing: '1px'
                            }}>
                                Power management is one of the core issues with providing peak display performance in high-ambient and
                                outdoor lighting environments. But with Sharp's leading-edge technologies, compensating for high power
                                requirements is a thing of the past. Our low-power, high-performance display solutions change the game,
                                enabling a whole new world of designs. Choose from Monochrome and 64-color<span><a style={{ color: 'black', textDecoration: 'underline' }} href="https://sharpsecd.com/#/MemoryInPixel"> Memory-In-Pixel (MIP) LCDs</a></span>
                                or full-color, high-resolution <span><a style={{ color: 'black', textDecoration: 'underline' }} href="https://sharpsecd.com/#/ReflectiveIGZO">Reflective IGZO displays.</a></span>
                            </p>
                            <div className="padding-top-50"></div>
                            <div style={{
                                width: '100%',
                                display: 'flex',
                                alignItems: 'center',
                                flexDirection: 'column'
                            }}>
                                <Button
                                    className="redButton"
                                    href="#/contact"
                                    size="lg"
                                    variant="outline-danger"
                                    style={{
                                        marginRight: 20,
                                        marginBlock: '3rem'
                                    }}
                                >
                                    Contact Sharp
                                </Button>
                            </div>
                            <div className="padding-top-50"></div>
                        </div>
                    </div>
                    <div
                        className="row"
                        style={{
                            maxWidth: '100%',
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "center",
                            flexDirection: "column",
                            paddingInline: '5rem',
                            paddingBlock: '2rem',
                            backgroundImage: `url(${Screen4})`,
                            backgroundSize: "cover",
                            backgroundPosition: "center center",
                            backgroundRepeat: "no-repeat",
                        }}
                    >

                        <div className="padding-top-50"></div>
                        <div className="padding-top-50"></div>

                        <div className="row">

                            <div tabIndex={0} className="col-sm-3">
                                <div style={{
                                    display: 'flex',
                                    flexDirection: 'column',
                                    alignItems: 'center',
                                    color: 'white'
                                }}>
                                    <img
                                        style={{ width: 90 }}
                                        src={outdoor}
                                        role="presentation"
                                        alt="icon"
                                    />
                                    <h1 style={{ fontSize: "1.3rem", padding: "1vw", textAlign: 'center' }}>
                                        <strong>OUTDOOR READABILITY</strong>
                                    </h1>
                                    <p style={{ padding: "1vw", textAlign: 'center' }}>
                                        from edge-of-vision to bright sunlight
                                    </p>
                                </div>
                            </div>

                            <div tabIndex={0} className="col-md-3">
                                <div style={{
                                    display: 'flex',
                                    flexDirection: 'column',
                                    alignItems: 'center',
                                    color: 'white'
                                }}>
                                    <img
                                        style={{ width: 90 }}
                                        src={lowPower}
                                        role="presentation"
                                        alt="icon"
                                    />
                                    <h1 style={{ fontSize: "1.3rem", padding: "1vw", textAlign: 'center' }}>
                                        <strong>LOW POWER</strong>
                                    </h1>
                                    <p style={{ padding: "1vw", textAlign: 'center' }}>
                                        No backlight required in reflective mode.
                                    </p>
                                </div>
                            </div>

                            <div tabIndex={0} className="col-md-3">
                                <div style={{
                                    display: 'flex',
                                    flexDirection: 'column',
                                    alignItems: 'center',
                                    color: 'white'
                                }}>
                                    <img
                                        style={{ width: 90 }}
                                        src={temperature}
                                        role="presentation"
                                        alt="icon"
                                    />
                                    <h1 style={{ fontSize: "1.3rem", padding: "1vw", textAlign: 'center' }}>
                                        <strong>WIDE TEMPERATURES</strong>
                                    </h1>
                                    <p style={{ padding: "1vw", textAlign: 'center' }}>
                                        Operates in even the most extreme environments
                                    </p>
                                </div>
                            </div>

                            <div tabIndex={0} className="col-md-3">
                                <div style={{
                                    display: 'flex',
                                    flexDirection: 'column',
                                    alignItems: 'center',
                                    color: 'white'
                                }}>
                                    <img
                                        style={{ width: 90 }}
                                        src={thinLightweight}
                                        role="presentation"
                                        alt="icon"
                                    />
                                    <h1 style={{ fontSize: "1.3rem", padding: "1vw", textAlign: 'center' }}>
                                        <strong>THIN + LIGHTWEIGHT</strong>
                                    </h1>
                                    <p style={{ padding: "1vw", textAlign: 'center' }}>
                                        Slim profile enables compact product design
                                    </p>
                                </div>
                            </div>


                        </div>
                        <div className="padding-top-30"></div>
                        <div className="padding-top-50"></div>
                        <div className="padding-top-50"></div>

                    </div>

                    <div
                        style={{
                            width: "100%",
                            justifyContent: "center",
                            display: "flex",
                            backgroundSize: "cover",
                            backgroundPosition: "center center",
                            backgroundRepeat: "no-repeat",
                        }}
                    >
                        <div
                            className="row"
                            style={{
                                maxWidth: "1140px",
                                display: "flex",
                                paddingTop: "50px",
                                paddingBottom: "100px",
                                alignItems: "center",
                                paddingRight: 20,
                                paddingLeft: 20,
                                margin: 0,
                            }}
                        >
                            <div style={{ width: "100%" }}>
                                <h1 style={{ fontSize: "3rem" }}>
                                    <strong>MEMORY IN PIXEL LCDs</strong>
                                </h1>
                                <div className="row" style={{ margin: 0, width: "100%" }}>
                                    <div
                                        className="col-sm-12 col-md-3"
                                        style={{
                                            height: 0,
                                            width: "100%",
                                            margin: "12px 0",
                                            border: "none",
                                            borderBottomWidth: "4px",
                                            borderBottomColor: "rgb(237,12,12)",
                                            borderBottomStyle: "solid",
                                        }}
                                    ></div>
                                </div>
                            </div>
                            <div
                                className="col-md-6"
                                style={{ paddingTop: 30, paddingBottom: 10 }}
                            >
                                <div>
                                    <img
                                        style={{ width: "100%", transform: 'scale(0.8)' }}
                                        src={image2}
                                        role="presentation"
                                        alt="Comparison with TFT"
                                    />
                                </div>
                            </div>
                            <div tabIndex={0} className="col-md-6">
                                <div tabIndex={-1} style={{ height: "100%", padding: 10 }}>
                                    <div
                                        className="centerAlignDiv"
                                        style={{
                                            flexDirection: "column",
                                            alignItems: "flex-start",
                                            borderBottom: 10,
                                        }}
                                    >
                                        <p>
                                            <strong>
                                                Choose from monochrome or 64-color for
                                                wearable and remote applications
                                            </strong>
                                        </p>
                                        <p>
                                            Our <span><a style={{ color: 'black', textDecoration: 'underline' }} href="https://sharpsecd.com/#/memory-in-pixel-lcds-technology">Memory-In-Pixel (MIP) technology </a></span> provides
                                            high-performance solutions for wearable and
                                            portable applications - or any product with a
                                            battery. Sizes range from 1.08-inch to 4.4-inch
(diagonal) with static-image operation at power
                                            levels as low as 1 O's of microamps
                                        </p>
                                        <div style={{
                                            display: 'flex',
                                            gap: '3rem'
                                        }}>
                                            <Button
                                                className="redButton"
                                                aria-label="Submit Form Button"
                                                style={{ borderRadius: 10, paddingInline: '1.5rem' }}
                                                variant="primary"
                                                href="https://sharpsecd.com/#/MemoryInPixel"
                                            >
                                                Learn More
                                            </Button>
                                            <Button
                                                className="blueButton"
                                                aria-label="Submit Form Button"
                                                style={{ borderRadius: 10, paddingInline: '1.5rem' }}
                                                variant="primary"
                                                href="https://sharpsecd.com/#/contact"
                                            >
                                                Request a Demo
                                            </Button>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        
                    </div>
                     <div
                        style={{
                            backgroundColor: "#d9d9d9",
                            width: "100%",
                            justifyContent: "center",
                            display: "flex",
                            backgroundSize: "cover",
                            backgroundPosition: "center center",
                            backgroundRepeat: "no-repeat",
                        }}
                    >
                        <div
                            className="row"
                            style={{
                                maxWidth: "1140px",
                                display: "flex",
                                paddingTop: "50px",
                                paddingBottom: "100px",
                                alignItems: "center",
                                paddingRight: 20,
                                paddingLeft: 20,
                                margin: 0,
                            }}
                        >
                            <div style={{ width: "100%" }}>
                                <h1 style={{ fontSize: "3rem" }}>
                                    <strong>R-IGZO LCDs</strong>
                                </h1>
                                <div className="row" style={{ margin: 0, width: "100%" }}>
                                    <div
                                        className="col-sm-12 col-md-3"
                                        style={{
                                            height: 0,
                                            width: "100%",
                                            margin: "12px 0",
                                            border: "none",
                                            borderBottomWidth: "4px",
                                            borderBottomColor: "rgb(237,12,12)",
                                            borderBottomStyle: "solid",
                                        }}
                                    ></div>
                                </div>
                            </div>
                            <div
                                className="col-md-6"
                                style={{ paddingTop: 30, paddingBottom: 30 }}
                            >
                                <div>
                                    <img
                                        style={{ width: "100%", transform: 'scale(0.8)' }}
                                        src={image4}
                                        role="presentation"
                                        alt="Comparison with TFT"
                                    />
                                </div>
                            </div>
                            <div tabIndex={0} className="col-md-6">
                                <div tabIndex={-1} style={{ height: "100%", padding: 10 }}>
                                    <div
                                        className="centerAlignDiv"
                                        style={{
                                            flexDirection: "column",
                                            alignItems: "flex-start",
                                        }}
                                    >
                                        <p>
                                            <strong>
                                                Full-color and high-resolution for hand
                                                held and signage applications
                                            </strong>
                                        </p>
                                        <p>
                                            Our <span><a style={{ color: 'black', textDecoration: 'underline' }} href="https://sharpsecd.com/#/ReflectiveIGZO">Reflective IGZO displays </a></span> combine 
                                            full-color
                                            and high resolution in a low-power reflective LCD
                                            Current available <span><a style={{ color: 'black', textDecoration: 'underline' }} href="https://sharpsecd.com/#/reflective-igzo-displays-product">sizes are 5.0-inch (diagonal)</a></span>  for
                                            hand-held, portable applications and <span><a style={{ color: 'black', textDecoration: 'underline' }} href="https://sharpsecd.com/#/reflective-igzo-displays-product">31.5-inch (diagonal)   </a></span>
targeted at outdoor signage
                                            applications. Both sizes include low-power
                                            backlighting for low-ambient viewing.
                                        </p>
                                        <div style={{
                                            display: 'flex',
                                            gap: '3rem'
                                        }}>
                                            <Button
                                                className="redButton"
                                                aria-label="Submit Form Button"
                                                style={{ borderRadius: 10, paddingInline: '1.5rem' }}
                                                variant="primary"
                                                href="https://sharpsecd.com/#/ReflectiveIGZO"
                                            >
                                                Learn More
                                            </Button>
                                            <Button
                                                className="blueButton"
                                                aria-label="Submit Form Button"
                                                style={{ borderRadius: 10, paddingInline: '1.5rem'  }}
                                                variant="primary"
                                                href="https://sharpsecd.com/#/contact"
                                            >
                                                Request a Demo
                                            </Button>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>


                    <div style={{
                        paddingInline: '10%'
                    }}>
                        <div className="padding-top-50"></div>
                        <div className="padding-top-50"></div>
                        <div className="padding-top-50"></div>
                        <h2 style={{
                            fontSize: '1.8rem',
                            fontWeight: 'bold'
                        }}>Sharp's low-power displays enable outstanding sunlight viewability at a fraction of the
                            power and cost</h2>
                        <div className="padding-top-50"></div>
                        <p style={{
                            fontSize: '1.6rem',
                        }}>
                            n conclusion, Sharp's low-power displays enable outstanding sunlight viewability at a fraction of the
                            power of other standard technologies historically used in outdoor or mobile applications. This saves
                            costs in terms of battery size and/or extensive thermal management techniques within the product.
                        </p>
                        <p style={{
                            fontSize: '1.6rem',
                        }}>
                            Options range from high-contrast mnonochrome and<span><a style={{ color: 'black', textDecoration: 'underline' }} href="https://sharpsecd.com/#/MemoryInPixel"> 64-color MIP displays</a></span> to full-color <span><a style={{ color: 'black', textDecoration: 'underline' }} href="https://sharpsecd.com/#/ReflectiveIGZO"> full-color, high-resolution R-IGZO displays</a></span>. tligb.:
                            These options address every type of content that might be needed in
                            outdoor and mobile applications
                        </p>
                    </div>
                    <div className="padding-top-50"></div>
                    <div className="padding-top-50"></div>
                </div>
                <div className="padding-top-30"></div>

                <div
                    ref={this.myRef}
                    style={{
                        backgroundColor: "rgba(45,47,65,1)",
                        width: "100%",
                        justifyContent: "center",
                        display: "flex",
                    }}
                >
                    <div
                        className="row"
                        style={{
                            maxWidth: "1140px",
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "center",
                            flexDirection: "column",
                            padding: 20,
                            margin: 0,
                        }}
                    >
                        <div>
                            <div className="padding-top-50 d-none d-md-block"></div>
                            <div className="padding-top-50"></div>
                            <h1
                                style={{
                                    fontSize: "3rem",
                                    textAlign: "center",
                                    color: "white",
                                }}
                            >
                                <strong>
                                    WANT TO KNOW MORE ABOUT SHARP LOW-POWER DISPLAYS?
                                </strong>
                            </h1>

                        </div>
                        <div style={{ width: "70%", marginTop: '5rem' }}>
                            <Form1 noValidate onSubmit={this.onSubmit}>
                                <Form1.Row style={{ paddingBottom: 10 }}>
                                    <Col>
                                        <Form1.Control
                                            aria-label="Email"
                                            placeholder="Email"
                                            aria-required={true}
                                            required
                                            type="email"
                                            onChange={(event) =>
                                                this.setState({ email: event.currentTarget.value })
                                            }
                                            value={this.state.email}
                                        />
                                        <Form1.Control.Feedback type="invalid">
                                            Please provide a valid email id.
                                        </Form1.Control.Feedback>
                                    </Col>
                                </Form1.Row>
                                <Form1.Row style={{ paddingBottom: 10 }}>
                                    <Col>
                                        <Form1.Control
                                            aria-required={true}
                                            aria-label="Name"
                                            required
                                            placeholder="Name"
                                            onChange={(event) =>
                                                this.setState({ name: event.currentTarget.value })
                                            }
                                            value={this.state.name}
                                        />
                                        <Form1.Control.Feedback type="invalid">
                                            Name field cannot be empty
                                        </Form1.Control.Feedback>
                                    </Col>
                                </Form1.Row>

                                <Form1.Row style={{ paddingBottom: 10 }}>
                                    <Col>
                                        <Form1.Control
                                            aria-label="Country Code"
                                            placeholder="+1"
                                            aria-required={true}
                                            required
                                            onChange={(event) =>
                                                this.setState({
                                                    mobileCode: event.currentTarget.value,
                                                })
                                            }
                                            value={this.state.mobileCode}
                                        />
                                        <Form1.Control.Feedback type="invalid">
                                            Country code field cannot be empty
                                        </Form1.Control.Feedback>
                                    </Col>
                                    <Col xs={10}>
                                        <Form1.Control
                                            aria-label="Contact Number"
                                            placeholder="Contact number"
                                            aria-required={true}
                                            required
                                            onChange={(event) =>
                                                this.setState({ mobile: event.currentTarget.value })
                                            }
                                            value={this.state.mobile}
                                            maxLength={10}
                                        />
                                        <Form1.Control.Feedback type="invalid">
                                            Phone number field cannot be empty
                                        </Form1.Control.Feedback>
                                    </Col>
                                </Form1.Row>
                                <Form1.Row>
                                    <Col>
                                        <Form1.Group>
                                            <Form1.Check
                                                className="customCheckbox"
                                                type="checkbox"
                                                label="Yes, I’d like to request a demo"
                                                checked={this.state.demo}
                                                onChange={(event) =>
                                                    this.setState({ demo: event.currentTarget.checked })
                                                }
                                            />
                                        </Form1.Group>
                                    </Col>
                                </Form1.Row>
                                <Form1.Row style={{ paddingBottom: 10 }}>
                                    <Col>
                                        <Form1.Control
                                            aria-label="Comment"
                                            aria-required={true}
                                            required
                                            as="textarea"
                                            rows="3"
                                            placeholder="Comments(optional)"
                                            onChange={(event) =>
                                                this.setState({ message: event.currentTarget.value })
                                            }
                                            value={this.state.message}
                                        />
                                        <Form1.Control.Feedback type="invalid">
                                            Message field cannot be empty
                                        </Form1.Control.Feedback>
                                    </Col>
                                </Form1.Row>
                                <Button
                                    className="redButton"
                                    aria-label="Submit Form Button"
                                    style={{ marginTop: 10, width: "100%" }}
                                    variant="primary"
                                    type="submit"
                                    disabled={this.state.submit}
                                >
                                    Submit
                                </Button>
                                {/* <div style={{ paddingBottom: 50 }}></div> */}
                                <Loader
                                    type="TailSpin"
                                    color="#e26565"
                                    height={50}
                                    width={50}
                                    timeout={0} //3 secs
                                    visible={this.state.showLoader}
                                    style={{ margin: "10px" }}
                                />
                                <Toast
                                    onClose={() =>
                                        this.setState({ show: false, submit: false })
                                    }
                                    show={this.state.show}
                                    className="toastSuccess"
                                // transition="Fade"
                                >
                                    <Toast.Header>
                                        <strong className="me-auto">Success</strong>
                                    </Toast.Header>
                                    <Toast.Body>
                                        Thank You! <br /> Your query has been successfully
                                        submitted.
                                    </Toast.Body>
                                </Toast>
                                <Toast
                                    onClose={() =>
                                        this.setState({ showError: false, submit: false })
                                    }
                                    show={this.state.showError}
                                    className="toastError"
                                // transition="Fade"
                                >
                                    <Toast.Header>
                                        <strong className="me-auto">Error</strong>
                                    </Toast.Header>
                                    <Toast.Body>
                                        We were unable to recieve your query. Please try again.
                                    </Toast.Body>
                                </Toast>
                                <div style={{ paddingBottom: 50 }}></div>
                                <p style={{ color: "white" }}>
                                    Looking for a smaller size class reflective displays?
                                    <a
                                        href="/"
                                        style={{ color: "#e61d24" }}
                                        onClick={(event) =>
                                            this.callFunct(event, "/memory-in-pixel-lcds-product")
                                        }
                                    >
                                        {" "}
                                        Visit our Memory in Pixel product page
                                    </a>
                                    .
                                </p>
                                <div style={{ paddingBottom: 50 }}></div>
                            </Form1>
                        </div>
                    </div>
                </div>
                <Footer />
            </div>
        );
    }
}