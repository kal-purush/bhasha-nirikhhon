import { colors } from "@/constants/Colors";
import { StyleSheet } from "react-native";

export const HomeStyles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
    width: "100%",
  },
  spinnerContainer: {
    width: "100%",
    justifyContent: "center",
    alignItems: "center",
    flex: 1,
  },
  roundContainer: {
    display: "flex",
    flexDirection: "row",
    justifyContent: "flex-start",
    alignItems: "flex-start",
    columnGap: 20,
    paddingVertical: 10,
    paddingHorizontal: 20,
    backgroundColor: colors.background.primary,
    borderBottomColor: colors.neutral.light,
    borderBottomWidth: 5,
  },
  whenConatiner: {
    display: "flex",
    flexDirection: "column",
    justifyContent: "flex-start",
    alignItems: "flex-start",
  },
  infoContainer: {
    display: "flex",
    flexDirection: "column",
    justifyContent: "flex-start",
    alignItems: "flex-start",
    rowGap: 10,
    flex: 1,
  },
  memberContainer: {
    display: "flex",
    flexDirection: "row",
    justifyContent: "flex-start",
    alignItems: "flex-start",
    columnGap: 10,
  },
  attributeContainer: {
    display: "flex",
    flexDirection: "row",
    justifyContent: "flex-start",
    alignItems: "flex-start",
    columnGap: 10,
  },
  attribute: {
    paddingVertical: 5,
    paddingHorizontal: 10,
    backgroundColor: colors.primary.default,
    borderRadius: 5,
  },
  attributeText: {
    color: colors.neutral.light,
  },
  scrollStyle: {
    width: "100%",
    backgroundColor: colors.background.primary,
  },
  roundCreatedTime: {
    position: "absolute",
    top: 0,
    right: 0,
  },
  roundTitle: {
    fontSize: 20,
    fontWeight: "bold",
  },
  roundDay: {
    fontSize: 16,
  },
  roundDayNumber: {
    fontSize: 24,
  },
  roundMonth: {
    fontSize: 16,
  },
});