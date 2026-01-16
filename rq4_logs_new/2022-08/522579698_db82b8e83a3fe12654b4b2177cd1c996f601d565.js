import { Container , Row , Col , Button } from 'react-bootstrap';
import "./style/SectionBelajar.css";

import Image from '../../img/people-figma.png';

export default function SectionBelajar(){
    return(
        <div className = "Section-Belajar">
            <Container>
                <Row>
                    <Col lg = { 5 } >
                        <h5>De-Talks</h5>
                        <h1>Masih Bingung Mulai Belajar Dari Mana?</h1>
                        <p>
                            Kami menyediakan HandBook sebagai panduan utama
                            untuk Anda mengenal berbagai profesi yang tersedia
                            di bidang design dan teknologi.
                        </p>
                        <Button> Baca Selengkapnya <i className = "fa fa-angle-right" /> </Button>
                    </Col>
                    <Col lg = { 7 } >
                        <img src = { Image } width = "100%" alt = "people-figma" />
                    </Col>
                </Row>
            </Container>
        </div>
    );
}