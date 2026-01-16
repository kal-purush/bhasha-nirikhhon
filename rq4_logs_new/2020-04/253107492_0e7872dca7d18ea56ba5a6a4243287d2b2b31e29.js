import React from "react"
import { useMediaQuery } from "react-responsive"

import SEO from "../components/seo"
import { NavBar, StyledLink } from "../styles/navigation"
import { Text } from "../styles/text"
import { breakpoints } from "../styles/layout"
import Issue1 from "../issues/issue-1"
import logo from "../images/logo.svg"

const Home = () => {
  const isPhoneWide = useMediaQuery({
    query: breakpoints.phoneWide,
  })

  return (
    <>
      <SEO title="Home" />
      <NavBar>
        <img
          width={isPhoneWide ? 75 : 128}
          src={logo}
          alt="small qrzn logo"
        ></img>
        {!isPhoneWide && <Text center>the quaranzine for the quaranzined</Text>}
        <StyledLink to="/">archive</StyledLink>
      </NavBar>

      <Issue1 />
    </>
  )
}

export default Home