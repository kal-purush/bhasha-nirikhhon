import { useEffect, useState } from "react";
import CastCss from "../css/CastMember.module.css";

function CastMember({ movieId }) {
  const apiKey = process.env.REACT_APP_MOVIE_INFO_KEY;
  const [castmember, setCastMember] = useState([]);

  const getCastMember = async () => {
    const json = await (
      await fetch(
        `https://api.themoviedb.org/3/movie/${movieId}/credits?api_key=${apiKey}`
      )
    ).json();
    console.log("캐스트멤버", json);
    setCastMember(json.cast);
  };

  useEffect(() => {
    getCastMember();
  }, []);

  return (
    <div className={CastCss.Container}>
      <h1 className={CastCss.title}>CastMember</h1>
      {castmember.slice(0, 10).map((member, index) => {
        return (
          <div key={index} className={CastCss.castGrid}>
            <img
              src={
                member.profile_path
                  ? `https://image.tmdb.org/t/p/w185${member.profile_path}`
                  : "null"
              }
              alt={member.name}
              className={CastCss.castImg}
            ></img>
            <div className={CastCss.castInfo}>
              <p className={CastCss.castName}>{member.name}</p>
              <p className={CastCss.castCharacter}>as {member.character}</p>
            </div>
          </div>
        );
      })}
    </div>
  );
}

export default CastMember;