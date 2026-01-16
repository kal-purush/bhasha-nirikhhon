// app/(screens)/dashboard.styles.ts
import { StyleSheet, Platform, StatusBar, Dimensions } from "react-native";

const { width, height } = Dimensions.get('window');

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#000",
  },
  keyboardAvoidingView: {
    flex: 1,
  },
  scrollViewContent: {
    flexGrow: 1,
    paddingBottom: 20,
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    paddingHorizontal: 20,
    paddingTop: Platform.OS === "android" ? StatusBar.currentHeight || 0 : 10,
    paddingBottom: 10,
  },
  profileContainer: {
    flexDirection: "row",
    alignItems: "center",
    backgroundColor: "rgba(255, 255, 255, 0.2)",
    borderRadius: 25,
    paddingRight: 15,
    paddingVertical: 2,
  },
  logoContainer: {
    flexDirection: "row",
    alignItems: "center",
    backgroundColor: "rgba(255, 255, 255, 0.2)",
    borderRadius: 25,
    paddingHorizontal: 15,
    paddingVertical: 8,
  },
  healthSyncText: {
    color: "white",
    fontSize: 16,
    fontWeight: "600",
    marginRight: 8,
  },
  avatarContainer: {
    marginRight: 8,
  },
  avatar: {
    width: 40,
    height: 40,
    borderRadius: 20,
    backgroundColor: "#ccc",
    justifyContent: "center",
    alignItems: "center",
  },
  avatarText: {
    color: "#333",
    fontWeight: "bold",
  },
  profileName: {
    color: "white",
    fontSize: 16,
    fontWeight: "500",
  },
  greetingContainer: {
    paddingHorizontal: 30,
    marginTop: 20,
    marginBottom: 15,
  },
  greetingText: {
    color: "white",
    fontSize: width > 350 ? 42 : 36,
    fontWeight: "bold",
    lineHeight: width > 350 ? 52 : 46,
  },
  // Weather section styles with health metrics
  weatherContainer: {
    paddingHorizontal: 20,
    marginBottom: 20,
  },
  weatherRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    backgroundColor: 'rgba(0, 0, 0, 0.3)',
    borderRadius: 15,
    padding: 15,
  },
  weatherLeftSection: {
    flex: 1,
  },
  locationSelector: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 8,
  },
  locationText: {
    color: 'white',
    fontSize: 14,
    marginHorizontal: 5,
  },
  weatherDetails: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  temperatureContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    marginRight: 10,
  },
  temperatureText: {
    color: 'white',
    fontSize: 20,
    fontWeight: 'bold',
    marginLeft: 6,
  },
  weatherTypeText: {
    color: 'white',
    fontSize: 14,
  },
  healthMetricsSection: {
    flex: 1.5,
  },
  metricsRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
  },
  metricItemColumn: {
    alignItems: 'center',
    flex: 1,
  },
  metricValue: {
    color: 'white',
    fontSize: 14,
    fontWeight: 'bold',
    marginTop: 4,
  },
  metricLabel: {
    color: 'rgba(255, 255, 255, 0.7)',
    fontSize: 12,
  },
  chatContainer: {
    marginHorizontal: 20,
    borderRadius: 20,
    overflow: "hidden",
    marginBottom: 15,
  },
  chatGradient: {
    padding: 20,
    borderRadius: 20,
  },
  chatHeader: {
    marginBottom: 20,
  },
  chatTitleRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  chatTitle: {
    color: "white",
    fontSize: 28,
    fontWeight: "400",
  },
  athenaText: {
    color: "#CCFF00",
    fontWeight: "500",
  },
  geminiImage: {
    width: 70,
    height: 30,
  },
  inputContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 10,
    paddingHorizontal: 15,
  },
  attachButton: {
    marginRight: 20,
  },
  textInputContainer: {
    flex: 1,
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: 'rgba(255, 255, 255, 0.15)',
    borderRadius: 25,
    paddingHorizontal: 15,
    height: 50,
  },
  textInput: {
    flex: 1,
    color: 'white',
    height: 50,
  },
  expandButton: {
    padding: 5,
  },
  sendButton: {
    padding: 5,
  },
  disclaimerText: {
    fontSize: 12,
    color: '#9DB5B2',
    textAlign: 'center',
    marginTop: 8,
  },
  tilesContainer: {
    paddingHorizontal: 20,
  },
  tilesRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    marginBottom: 20,
  },
  errorText: {
    color: '#FF6B6B',
    textAlign: 'center',
  },
  tile: {
    width: "48%",
    aspectRatio: 1.5,
    backgroundColor: "rgba(0, 0, 0, 0.6)",
    borderRadius: 20,
    justifyContent: "center",
    alignItems: "center",
  },
  tileText: {
    color: "white",
    fontSize: 18,
    fontWeight: "500",
    marginTop: 10,
  },
});

export default styles;