import java.io.*;

public class AudioSteganographyDecoderTxt {

    // Đọc nội dung từ file txt
    public static String readFileContent(String filePath) throws IOException {
        StringBuilder content = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                content.append(line).append("\n");
            }
        }
        return content.toString().trim();
    }

    // Hàm giải mã để trích xuất thông điệp bí mật
    public static String extractData(String inputAudioPath) throws IOException {
        File inputFile = new File(inputAudioPath);
        FileInputStream fis = new FileInputStream(inputFile);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Đọc toàn bộ nội dung tệp âm thanh
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = fis.read(buffer)) != -1) {
            baos.write(buffer, 0, bytesRead);
        }
        fis.close();
        byte[] audioBytes = baos.toByteArray();

        // Duyệt qua các byte bắt đầu từ byte 44 (vì header 44 byte)
        StringBuilder secretBits = new StringBuilder();
        for (int i = 44; i < audioBytes.length; i++) {
            // Lấy LSB của byte hiện tại
            secretBits.append(audioBytes[i] & 0x01);
        }

        // Chuyển các bit thành chuỗi ký tự
        StringBuilder secretMessage = new StringBuilder();
        for (int i = 0; i < secretBits.length(); i += 8) {
            // Lấy 8 bit một lần để tạo ra một byte
            String byteStr = secretBits.substring(i, Math.min(i + 8, secretBits.length()));
            char c = (char) Integer.parseInt(byteStr, 2);
            secretMessage.append(c);

            // Nếu gặp dấu "###" (kết thúc thông điệp), dừng lại
            if (secretMessage.toString().endsWith("###")) {
                break;
            }
        }

        // Trả lại thông điệp đã giải mã (loại bỏ "###")
        return secretMessage.toString().replace("###", "");
    }

    public static void main(String[] args) {
        String inputAudioPath = "output.wav";  // Tệp âm thanh đã được nhúng thông điệp
        try {
            // Giải mã thông điệp từ tệp âm thanh
            String secretMessage = AudioSteganographyDecoderTxt.extractData(inputAudioPath);
            System.out.println("Thông điệp giải mã được: " + secretMessage);
        } catch (IOException e) {
            System.err.println("Lỗi khi giải mã: " + e.getMessage());
        }
    }
}