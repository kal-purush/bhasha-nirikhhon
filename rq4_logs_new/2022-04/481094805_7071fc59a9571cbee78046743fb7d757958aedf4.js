import React, { useState } from "react";
import 'bootstrap/dist/css/bootstrap.css';
import { Alert, Button } from "react-bootstrap";

const AlertComponent = () => {
    const [show, setShow] = useState(true);

    return (
        <div>
            <div class="row d-flex justify-content-center text-center">
                <h1>React bootstrap alert notification - technostuf.com</h1>
                <hr />
                <div className="col-md-8">
                    <h4>Success Alert</h4>
                    <Alert variant="success">
                        <Alert.Heading>This is heading</Alert.Heading>
                        <p>
                            Ab at officiis, condimentum, lorem dolore enim? Platea numquam soluta reiciendis, eligendi aptent lacinia fermentum vitae lectus molestias iusto sed eum netus! Sociis quisquam nec.
                        </p>
                    </Alert>
                </div>
                <div className="col-md-8">
                    {show ? (
                        <>
                            <h4>Warning Alert</h4>
                            <Alert variant="warning" onClose={() => setShow(false)} dismissible>
                                <Alert.Heading>This is heading</Alert.Heading>
                                <p>
                                    Aww yeah, you successfully read this important alert message. This example
                                    text is going to run a bit longer so that you can see how spacing within an
                                    alert works with this kind of content.
                                </p>
                            </Alert>
                        </>
                    ) : (
                        <Button onClick={() => setShow(true)}>Show Alert</Button>
                    )};
                </div>

                <div className="col-md-8">
                    <h4>Secondary Alert</h4>
                    <Alert variant="secondary">
                        <Alert.Heading>This is heading</Alert.Heading>
                        <p>
                            Commodo malesuada consectetur quo, vulputate dignissim ad rutrum, dolor, veritatis. Pariatur? Veniam, quasi minima! Eligendi sollicitudin maxime quam non aliquet convallis quae condimentum cras suspendisse.
                        </p>
                    </Alert>
                </div>

                <div className="col-md-8">
                    <h4>Primary Alert</h4>
                    <Alert variant="primary">
                        <Alert.Heading>This is heading</Alert.Heading>
                        <p>
                            Voluptates laborum, aliquet vestibulum recusandae voluptatem debitis, faucibus fugiat, arcu, ex provident. Vestibulum etiam anim voluptas. Consequatur distinctio repellat lobortis est aut, quasi do? Vehicula.
                        </p>
                    </Alert>
                </div>

                <div className="col-md-8">
                    <h4>Danger Alert</h4>
                    <Alert variant="danger">
                        <Alert.Heading>This is heading</Alert.Heading>
                        <p>
                            Voluptate voluptatem nisi commodo veritatis porttitor? Distinctio eaque, iste varius. Quisque illum expedita iure ab. Rerum imperdiet dolor totam convallis, veniam dignissimos at optio, dis.
                        </p>
                    </Alert>
                </div>



                <div className="col-md-8">
                    <h4>Light Alert</h4>
                    <Alert variant="light">
                        <Alert.Heading>This is heading</Alert.Heading>
                        <p>
                            Curabitur quod varius habitasse laboris vestibulum? Necessitatibus expedita aptent ipsa, eget torquent curabitur dapibus asperiores modi molestie tincidunt ipsa egestas suscipit consequuntur omnis eu, convallis.
                        </p>
                    </Alert>
                </div>

                <div className="col-md-8">
                    <h4>Dark Alert</h4>
                    <Alert variant="dark">
                        <Alert.Heading>This is heading</Alert.Heading>
                        <p>
                            Hic minus proin nobis? Pharetra possimus libero voluptas, urna eligendi, quia enim auctor explicabo ea veritatis. Aperiam sociis? Elementum primis pharetra architecto quod doloribus consequat.
                        </p>
                    </Alert>
                </div>

                <div className="col-md-8">
                    <h4>Info Alert</h4>
                    <Alert variant="info">
                        <Alert.Heading>This is heading</Alert.Heading>
                        <p>
                            Hic minus proin nobis? Pharetra possimus libero voluptas, urna eligendi, quia enim auctor explicabo ea veritatis. Aperiam sociis? Elementum primis pharetra architecto quod doloribus consequat.
                        </p>
                    </Alert>
                </div>
            </div>
        </div>
    )
}
export default AlertComponent;