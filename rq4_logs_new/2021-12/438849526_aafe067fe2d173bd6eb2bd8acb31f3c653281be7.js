import {
  Container,
  Heading,
  Text,
  Box,
  useColorModeValue,
  LinkBox,
  Stack,
  AspectRatio,
  Image,
  VStack,
  LinkOverlay,
  HStack,
  Icon,
} from "@chakra-ui/react";
import Section from "../components/section";
import Link from "next/link";
import gearData from "../data/datagear.json";
import { ExternalLinkIcon } from "@chakra-ui/icons";
const GearPage = () => {
  return (
    <Container maxW="container.md">
      <Section>
        <Heading mt={20} variant="section-title">
          Gear.
        </Heading>
        <Text>
          Her er en liste over det hardware jeg bruger til hverdag.{" "}
          <strong>Links er ikke affiliated.</strong> For at se hvilke
          redskaber/sprog jeg bruger, gå tilbage til{" "}
          <Link href="/#toolsPage" passHref>
            <Box
              display="inline-block"
              color={useColorModeValue("purple.400", "red.200")}
              cursor="pointer"
            >
              forsiden
            </Box>
          </Link>
        </Text>
      </Section>
      <Section>
        {gearData &&
          gearData.gearData.map(({ title, image, description, url }) => (
            <>
              <LinkBox role="group" as="article" my={10}>
                <Stack
                  direction={{ base: "column", md: "row" }}
                  p={3}
                  spacing={6}
                  justifyContent={{ base: "center", md: "flex-start" }}
                  alignItems="center"
                  w="full"
                  my={5}
                >
                  <AspectRatio ratio={1} w="80px">
                    <Image alt={title} src={image} layout="fill" />
                  </AspectRatio>
                  <VStack spacing={0} alignItems="flex-start" flex={1} w="full">
                    <LinkOverlay href={url} isExternal>
                      <Heading pb={2} size="md">
                        {title}
                      </Heading>
                    </LinkOverlay>
                    <Text fontSize="smd">{description}</Text>
                  </VStack>
                  <HStack w={12} justifyContent="flex-start">
                    <Icon
                      boxSize={6}
                      as={ExternalLinkIcon}
                      // eslint-disable-next-line react-hooks/rules-of-hooks
                      color={useColorModeValue("purple.300", "red.200")}
                      opacity={0}
                      _groupHover={{ ml: 6, opacity: 1 }}
                      transitionProperty="all"
                      transitionDuration="slow"
                      transitionTimingFunction="ease-out"
                    />
                  </HStack>
                </Stack>
              </LinkBox>
            </>
          ))}
      </Section>
    </Container>
  );
};

export default GearPage;