package Happy20.GrowingPetPlant.Subscribe;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Subscriber {
    private static final Logger log = LoggerFactory.getLogger(Subscriber.class);
    private static Subscriber instance;
    private MqttClient client;
    private MqttConnectOptions connOpts;

    private static final String DB_URL = "jdbc:mysql://localhost:3306/GPP?serverTimezone=Asia/Seoul&characterEncoding=UTF-8";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "6797";

    // Private 생성자로 외부에서 인스턴스 생성을 막음
    private Subscriber() {}

    // 인스턴스 반환 메서드
    public static synchronized Subscriber getInstance() {
        if (instance == null) {
            instance = new Subscriber();
        }
        return instance;
    }

    // 초기화 메서드
    public void initialize(String broker, String clientId) {
        try {
            client = new MqttClient(broker, clientId, new MemoryPersistence());
            connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setAutomaticReconnect(true); // 자동 재연결 활성화
            connOpts.setConnectionTimeout(10); // 연결 시간 초과 설정 (옵션)
            connOpts.setKeepAliveInterval(60); // Keep Alive 간격 설정 (옵션)
        } catch (MqttException e) {
            log.error("Failed to initialize subscriber", e);
        }
    }

    // 구독 메서드
    public void subscribe(String topic, int qos) {
        try {
            if (!client.isConnected()) {
                log.warn("Client is not connected. Subscribing will be delayed.");
                return;
            }
            log.info("Subscribing to topic: " + topic);
            client.subscribe(topic, qos);
        } catch (MqttException e) {
            log.error("Failed to subscribe to topic", e);
        }
    }

    // 연결 메서드
    public void connect() {
        try {
            log.info("Connecting to broker: " + client.getServerURI());
            client.connect(connOpts);
            log.info("Connected");
        } catch (MqttException e) {
            log.error("Failed to connect to broker", e);
        }
    }

    // 콜백 설정 메서드
    public void setCallback(MqttCallback callback) {
        client.setCallback(callback);
    }

    // 데이터베이스에 데이터 삽입 메서드
    public void insertToDatabase(String tempPayload, String humiPayload, String soilPayload) {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            // 데이터를 삽입할 테이블 이름
            String query = "INSERT INTO STATUS (temperature, humidity, moisture) VALUES (?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                // 각 토픽의 값을 쿼리에 설정
                pstmt.setString(1, tempPayload);
                pstmt.setString(2, humiPayload);
                pstmt.setString(3, soilPayload);
                pstmt.executeUpdate();
            }
        } catch (SQLException e) {
            log.error("Failed to insert data into database", e);
        }
    }
}
