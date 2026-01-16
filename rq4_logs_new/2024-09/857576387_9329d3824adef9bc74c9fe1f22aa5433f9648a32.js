import { View, Text, TouchableOpacity, TouchableWithoutFeedback, ScrollView, Image } from "react-native";
import React from "react";
import { useNavigation } from "@react-navigation/native";

const MovieList = ({ title, data, horizontal = true }) => {
  const navigation = useNavigation();

  return (
    <View className="mb-8 space-y-4">
      <View className="mx-4 flex-row justify-between items-center">
        <Text className="text-white text-xl font-bold">{title}</Text>
        <TouchableOpacity>
          <Text className="text-primary">See All</Text>
        </TouchableOpacity>
      </View>
      <ScrollView horizontal={horizontal} showsHorizontalScrollIndicator={false} contentContainerStyle={{ paddingHorizontal: 15 }}>
        {data.map((item, index) => (
          <TouchableWithoutFeedback key={index} onPress={() => navigation.navigate("Anime", item)}>
            <View className="mr-4 flex-col items-center">
              <Image source={{ uri: item.image }} className="w-40 h-60 rounded-lg" />
              <Text className="text-white text-sm mt-2 text-center">{item.title.length > 14 ? item.title.slice(0, 14) + "..." : item.title}</Text>
            </View>
          </TouchableWithoutFeedback>
        ))}
      </ScrollView>
    </View>
  );
};

export default MovieList;