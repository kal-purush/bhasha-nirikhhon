import { StyleSheet } from "react-native"
import Colors from '../../Utils/Colors';

const styles = StyleSheet.create({
  header: {
    flexDirection: "row",
    height: 110,
    backgroundColor: Colors.MALTE,
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingLeft: 10,
    paddingRight: 10,
  },
  iconBack: {
    marginTop: 30,
    color: Colors.BRANCO,
  },
  imgLogo: {
    width: 50,
    height: 70,
    marginTop: 35,
  },
  constainerBusca:{
    height: 100, 
    alignItems: 'center',
    alignContent: 'center', 
    backgroundColor: Colors.MALTE,
    borderBottomRightRadius: -30,
    borderBottomLeftRadius: -30,
  },
  viewBusca: {
    flexDirection: "row",
    justifyContent: "center",
    alignSelf: 'center',
    backgroundColor: Colors.BRANCO,
    width: "90%",
    height: 40,
    margin: 10,
    borderRadius: 20,
    marginTop: 20
  },
  sectionHeader:{
    fontSize: 15,
    fontWeight: 'bold',
    marginLeft: 15,
    color: Colors.AZUL_MARINHO
  },
  containerJobs: {
    borderTopRightRadius: 30,
    borderTopLeftRadius: 30,
  },
  
  lineSeparator: {
    height: 1,
    width: "90%",
    backgroundColor: Colors.MALTE,
  },
  inputBusca: {
    flex: 1,
    borderRadius: 25,
    fontSize: 15,
    paddingHorizontal: 15,

  },
  boxTitle: {
    flexDirection: "row",
    justifyContent: "space-between",
    paddingTop: 10,
    marginLeft: 20,
    marginRight: 30,
  },
  textTitle: {
    color: Colors.MALTE,
    fontSize: 25,
    marginBottom: 10,
  },
  textFiltrar: {
    marginTop: 20,
    color: Colors.AZUL_PACIFICO,
  },
  viewTitle: {
    width: "50%",
    padding: 2,
    marginHorizontal: 150,
    backgroundColor: Colors.AZUL_CELESTE,
  },
  item: {
    flexDirection: 'row',
    margin: 10,
    backgroundColor: Colors.BRANCO,
    width: "95%",
    padding: 15,
    borderRadius: 20,
  },
  image: {
    width: 90,
    height: 90,
    borderRadius: 10,
  },
  textContainer: {
    marginLeft: 15,
    width:"50%",
  },
  title: {
    fontSize: 16,
    color: Colors.MALTE,
    maxWidth: "100%",
    fontWeight: 'bold',
  },
  subtitle: {
    marginTop: 10,
    fontSize: 12,
    color: Colors.MALTE,
    maxWidth: "100%",
  },
  Button:{
    backgroundColor: Colors.AZURE,
    maxWidth: 80,
    width: 50,
    height: 20,
    borderRadius: 20,
    alignItems: 'center',
    marginTop: 70,
  },
  buttomText:{
    color: Colors.BRANCO,
  },
  NameSu:{
    color: Colors.MALTE
  },
  ContainerImage:{
    alignItems: 'center'

  },
});
export default styles