import { Button, Checkbox, Flex, Form, Input } from "antd";
import star from "../assets/star.png";
import axios from "axios";
import Swal from "sweetalert2";
import { useNavigate, useRoutes } from "react-router-dom";
import Title from "antd/es/typography/Title";
import FeedCard from "./FeedCard";

export default function CardList({ data }) {
  return (
    <Flex vertical gap={"medium"}>
      {data.map((item, index) => (
        <FeedCard key={item.id} data={item} />
      ))}
    </Flex>
  );
}