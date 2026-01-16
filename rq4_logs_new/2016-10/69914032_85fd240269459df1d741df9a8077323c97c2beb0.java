class BarcodeServletTest {

    @Test public void test() {

        BarcodeServlet servlet = new BarcodeServlet();

        params.put("height", "200");
        params.put("width", "3");
        params.put("resolution", "72");

        req.setParameters(params);

        servlet.doGet(req, res);

        Barcode barcode = servlet.getBarcode();
        assertEquals(72, barcode.getResolution());
    }
}