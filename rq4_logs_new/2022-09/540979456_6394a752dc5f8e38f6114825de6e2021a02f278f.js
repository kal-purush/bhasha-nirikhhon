import { Button, HStack, Image, ScrollView, Text, VStack } from "native-base";
import React from "react";
import MovieItem from "../ListItemFolder/MovieItem";
import PersonItem from "../ListItemFolder/PersonItem";
import TVItem from "../ListItemFolder/TVItem";

export default function MultiList({ navigation, multiList }) {
  function RenderCardAsPerType(multiListItem) {
    if (multiListItem.media_type === "movie") {
      return <MovieItem navigation={navigation} ml={multiListItem} />;
    } else if (multiListItem.media_type === "tv") {
      return <TVItem navigation={navigation} tvShow={multiListItem} />;
    } else {
      console.log(multiListItem);
      return <PersonItem navigation={navigation} pItem={multiListItem} />;
    }
  }
  return (
    <ScrollView>
      {multiList.map((mv, idx) => {
        return RenderCardAsPerType(mv);
      })}
    </ScrollView>
  );
}