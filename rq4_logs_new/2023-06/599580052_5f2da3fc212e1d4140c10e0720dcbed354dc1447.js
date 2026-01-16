import { View,Text, StyleSheet, Pressable, FlatList, ScrollView, ToastAndroid, Alert, SafeAreaView } from 'react-native'
import React, { useEffect, useState } from 'react'
import MYinputs from './components/MYinputs';
import mstyle from './mstyle';
import Buttons from './components/Buttons';
import { AuthenicationService } from './services';
import submitReqData from './services/FormData';
import { back } from 'react-native/Libraries/Animated/Easing';
import { ActivityIndicator } from 'react-native';
import SearchableDropDown from 'react-native-searchable-dropdown'

import Icon from 'react-native-vector-icons/Ionicons';

const ActivityDealerDetailsScreen = ({ navigation}) => {
 
  const [activity_type, setactivity_type] = useState([])
  const [formdata, setformdata] = useState([ ])
  const [isLoading, setisLoading] = useState(false)
  const [isFormLoading, setisFormLoading] = useState(false)
  const [selectedDelers, setselectedDelers] = useState()
  const [dealers, setdealers] = useState([])

  useEffect(() => {
    setisFormLoading(true)
    let req = {

    }
    AuthenicationService.activity_for(req).then(res => {
    //   console.log(res.data)
      if (res?.status == true) {
        mapped_array = []
        res.data.forEach(a => {
          mapped_array.push(a.json )
        })
        setisFormLoading(false)
        // console.log(JSON.parse(mapped_array))
        setformdata(JSON.parse(mapped_array))
      } else {
        setisFormLoading(false)
      }
    })
    getDealers()

  }, [])
  
  const getDealers = () => {
    let req = {
      "text": ''
    }
    AuthenicationService.searchdealerData(req)
      .then(x => {
        // console.log("kjh sakfsj",x.data)
        if (x.status == true) {
          let mapped_array = []
          x.data.forEach(a => {
            mapped_array.push({ "name": `${a.dealer_name}`, "id": a.mobile_number, "qr_code": a.qr_code })
          })
          setdealers(mapped_array)
        } else {
        }
      })
      .catch(error => {
        // console.log(error)
      })
  }

//   if (activity_type.length===0){
//     // console.log(activity_type.length)
//     let req = {
        
//     }
//     AuthenicationService.activity_type(req).then(res => {
//       // // console.log(res.data)
//       if (res?.status == true) {
//         mapped_array=[]
//         res.data.forEach(a=> {
//           mapped_array.push( a.name)
//         })
//         formdata[1].options =mapped_array
//         setactivity_type(mapped_array)
//       } else {
//       }
//     })

//   }


  const submit = () => {
    // // console.log(formdata)
    let req = submitReqData(formdata);
    setisLoading(true);
    console.log(req)
    req.dealer = selectedDelers?.id

    if(!req.image){
req.image=''
    }

    if(req.dealer ==''){
            Alert.alert('Alert Empty Field',`Please Select Dealer`)
            return
    }


    formdata.forEach(a => {
        if (a?.req===1 && a?.value===''){
            Alert.alert('Alert Empty Field',`Please Fill ${a?.label}`)
            return
        }
      })

    AuthenicationService.create_activity(req).then(response => {
      setisLoading(false);
      console.log(response)
      if (response?.status == true) {
        ToastAndroid.showWithGravityAndOffset(
          response?.message,
          ToastAndroid.LONG, ToastAndroid.BOTTOM, 25, 50)
        navigation.goBack()
      } else {
        ToastAndroid.showWithGravityAndOffset(
          response?.message,
          ToastAndroid.LONG, ToastAndroid.BOTTOM, 25, 50
        );

      }
    }).catch(e=>{
      // console.log(e)
    })

  }

  const update = () => {
    // console.log(formdata)
  }


  return (
    <SafeAreaView style={mstyle.container1}>
    {isFormLoading?(

<View style={{ flex: 1, justifyContent: 'center', alignItems: 'center' }}>
<ActivityIndicator size="large" />
</View>
    ):(
        <View>
              <View style={[mstyle.inputContainer1, {
                    backgroundColor: 'white', marginTop: 7,

                  }]}>
                    <Text style={{ color: 'black' }}> Select Dealer</Text>
                    <View style={mstyle.inputSubContainer}>
                      {selectedDelers ? (
                        <View style={{
                          padding: 8, marginTop: 2, flexDirection: 'row',
                          backgroundColor: 'white', borderColor: 'silver',
                          borderWidth: 1, borderRadius: 5, width: '100%'
                        }}>

                          <Text style={{ color: 'black', width: '90%', fontSize: 15, fontWeight: 'bold' }}> {selectedDelers.name}</Text>
                          <Icon onPress={() => { setselectedDelers() }} name='close-circle-outline' size={25} style={{ color: 'red' }}></Icon>

                        </View>
                      ) : (
                        <SearchableDropDown
                          onItemSelect={(item) => {
                            // const items = selectedCrops;
                            // items.push(item)
                            setselectedDelers(item)
                            // console.log(selectedDelers)
                          }}
                          containerStyle={{ padding: 3, width: '100%' }}
                          onRemoveItem={(item, index) => {
                            // const items = selectedCrops.filter((sitem) => sitem.name !== item.name);
                            // setselectedDelers(items)
                          }}
                          itemStyle={{
                            padding: 10,
                            marginTop: 2,
                            backgroundColor: 'white',
                            borderColor: 'silver',
                            borderWidth: 1,
                            borderRadius: 5,
                          }}
                          itemTextStyle={{ color: '#222' }}
                          itemsContainerStyle={{ maxHeight: 140 }}
                          items={dealers}
                          defaultIndex={2}
                          resetValue={false}
                          textInputProps={
                            {
                              placeholder: "Search Dealer",
                              underlineColorAndroid: "transparent",
                              style: {
                                padding: 8,
                                borderWidth: 1,
                                borderColor: '#ccc',
                                borderRadius: 5,
                                color: "black"
                              },
                              onTextChange: text => {

                              }
                            }
                          }
                          listProps={
                            {
                              nestedScrollEnabled: true,
                            }
                          }
                        />
                      )}

                      {/* <TextInput
                    placeholder={'Type something'}
                    placeholderTextColor={Colors.DEFAULT_GREY}
                    selectionColor={Colors.DEFAULT_GREY}
                    style={mstyle.inputText}
                    onChangeText={text => {
                      searchFilterFunction(text)
                    }}
                  /> */}
                    </View>
                  </View>
        <FlatList
            data={formdata}
            style={{marginBottom:4}}
            renderItem={({ item, index }) => {
              return (
               
                  <MYinputs item={item} />
    
              )
            }}
            ListFooterComponent={()=>{
              return(
                <Pressable style={{marginBottom:10}} onPress={() => { submit() }}>
                <Buttons title={'Submit'} loading={isLoading} />
              </Pressable>
              )
            }}
            
            
            />
    
        
    
    
        </View>
    )}

    </SafeAreaView>
  )
}


const styles = StyleSheet.create({
  InputStyle: {
    padding: 20, shadowColor: 'rgba(0,0,0,0.4)',
    shadowOffset: {
      width: 1,
      height: 2,
    },
    shadowOpacity: 0.34,
    shadowRadius: 6.27,
    elevation: 2,
  },

  inputbox: {
    paddingHorizontal: 10,
    paddingVertical: 15
  },
  images: {
    width: 100,
    height: 100,
    borderColor: 'black',
    borderWidth: 1,
    marginHorizontal: 3
  },
})
export default ActivityDealerDetailsScreen