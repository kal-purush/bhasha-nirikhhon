import axios from "axios";
import geojson from "../../../public/coords.json";
import { Spin } from "antd";
import { IoMdCloseCircle } from "react-icons/io";
import { CgMenuRound } from "react-icons/cg";
import { useEffect, useState, useRef } from "react";
import Layout from "../../components/common/mapLayout";
import { CustomOverlayMap, Map, Polygon, MapMarker } from "react-kakao-maps-sdk";
import { CateTitle, Category, CloseBtn, Container, DivArea, Infra, MapWrap, MenuBtn, Search, SideBar, SideWrap, WaitBox } from "@/styles/map1.styles";

const map = () => {
    let sideBar = useRef(null);
    let menuBtn = useRef(null);

    // 버튼 배경색, 글자색 바꿀 변수
    let libBtn = useRef(null);
    let parkBtn = useRef(null);
    let hosBtn = useRef(null);
    let gymBtn = useRef(null);
    let swimBtn = useRef(null);
    let colBtn = useRef(null);
    let marBtn = useRef(null);
    let rehBtn = useRef(null);
    let oldBtn = useRef(null);
    let kidsBtn = useRef(null);

    const typeBtn = {
        library: libBtn,
        park: parkBtn,
        hospital: hosBtn,
        gym: gymBtn,
        swim: swimBtn,
        reh: rehBtn,
        college: colBtn,
        market: marBtn,
        old: oldBtn,
        kids: kidsBtn,
    };

    // 공공데이터에서 받아오는 정리안된 초기 데이터 배열
    const [hos, setHos] = useState([]);
    const [lib, setLib] = useState([]);
    const [park, setPark] = useState([]);
    const [swim, setSwim] = useState([]);
    const [gym, setGym] = useState([]);
    const [col, setCol] = useState([]);
    const [mar, setMar] = useState([]);
    const [reh, setReh] = useState([]);
    const [old, setOld] = useState([]);
    const [kids, setKids] = useState([]);

    // 공공데이터에서 받아온 초기 데이터를 정리해서 이름, 좌표, 구이름, 동이름 넣어줄 배열
    const [hosInfo, setHosInfo] = useState([]);
    const [libInfo, setLibInfo] = useState([]);
    const [parkInfo, setParkInfo] = useState([]);
    const [swimInfo, setSwimInfo] = useState([]);
    const [gymInfo, setGymInfo] = useState([]);
    const [colInfo, setColInfo] = useState([]);
    const [marInfo, setMarInfo] = useState([]);
    const [rehInfo, setRehInfo] = useState([]);
    const [oldInfo, setOldInfo] = useState([]);
    const [kidsInfo, setKidsInfo] = useState([]);

    // infra cnt값 변수 생성
    let hosClickCnt = useRef(0);
    let libClickCnt = useRef(0);
    let parkClickCnt = useRef(0);
    let swimClickCnt = useRef(0);
    let gymClickCnt = useRef(0);
    let colClickCnt = useRef(0);
    let marClickCnt = useRef(0);
    let rehClickCnt = useRef(0);
    let oldClickCnt = useRef(0);
    let kidsClickCnt = useRef(0);

    let info = [];

    const [map, setMap] = useState(null);
    const [dongInfo, setDongInfo] = useState([]); // 동의 폴리곤 정보들이 다 들어있음
    const [markers, setmarkers] = useState([]); // 지도에 최종적으로 띄울 동의 폴리곤 정보 넣어줄 배열
    const [selectedDong, setSelectedDong] = useState([]);
    const [mousePosition, setMousePosition] = useState({
        lat: 0,
        lng: 0,
    });

    let waitBox = useRef(null);
    const [display, setDisplay] = useState("");

    // 버튼2를 클릭했을때 사용할 인프라 정보들을 넣어둔 객체
    const btn = {
        hospital: [hos, "DUTYADDR", setHosInfo, hosClickCnt],
        library: [lib, "ADRES", setLibInfo, libClickCnt],
        park: [park, "P_ADDR", setParkInfo, parkClickCnt],
        swim: [swim, "SITEWHLADDR", setSwimInfo, swimClickCnt],
        gym: [gym, "ADDR_OLD", setGymInfo, gymClickCnt],
        college: [col, "ADD_KOR", setColInfo, colClickCnt],
        market: [mar, "ITEM_ADDR", setMarInfo, marClickCnt],
        reh: [reh, "FCLT_ADDR", setRehInfo, rehClickCnt],
        old: [old, "FCLT_ADDR", setOldInfo, oldClickCnt],
        kids: [kids, "BASS_ADRES", setKidsInfo, kidsClickCnt],
    };

    const dataType = {
        hospital: hosInfo,
        library: libInfo,
        park: parkInfo,
        swim: swimInfo,
        gym: gymInfo,
        college: colInfo,
        market: marInfo,
        reh: rehInfo,
        old: oldInfo,
        kids: kidsInfo,
    };

    const name = {
        hospital: "DUTYNAME",
        library: "LBRRY_NAME",
        park: "P_PARK",
        swim: "BPLCNM",
        gym: "NM",
        college: "NAME_KOR",
        market: "ITEM_NM",
        reh: "FCLT_NM",
        old: "FCLT_NM",
        kids: "FCLTY_NM",
    };

    const [selectedType, setSelectedType] = useState([]);

    // 데이터 요청
    useEffect(() => {
        axios
            .all([
                axios.get(`http://openapi.seoul.go.kr:8088/576e61714e636a6b3637545a455046/json/TvEmgcHospitalInfo/1/500/`),
                axios.get(`http://openapi.seoul.go.kr:8088/4f49455041636a6b37376d485a5255/json/SeoulPublicLibraryInfo/1/500/`),
                axios.get(`http://openAPI.seoul.go.kr:8088/6258655663626f623130365855576c6d/json/SearchParkInfoService/1/130/`),
                axios.get(`http://openapi.seoul.go.kr:8088/4b56466d77626f6234384444716670/json/LOCALDATA_103501/1/500/`),
                axios.get(`http://openapi.seoul.go.kr:8088/6f6e754d72626f6238336d58554962/json/InnerGmnsmInfo/1/500/`),
                axios.get(`http://openapi.seoul.go.kr:8088/4643455575626f623933496f444f53/json/SebcCollegeInfoKor/1/500/`),
                axios.get(`http://openAPI.seoul.go.kr:8088/4f46506f71626f6231366262796977/json/ListTraditionalMarket/1/500/`),
                axios.get(`http://openapi.seoul.go.kr:8088/494b465849626f6236316e786c4f57/json/fcltOpenInfo_OMSI/1/500/`),
                axios.get(`http://openapi.seoul.go.kr:8088/5945797941626f623930564f595467/json/fcltOpenInfo_OWI/1/500/`),
                axios.get(`http://openapi.seoul.go.kr:8088/6851706553626f623130307049427043/json/tnFcltySttusInfo1011/1/500/`),
            ])
            .then(
                axios.spread((hos, lib, park, swim, gym, col, mar, reh, old, kids) => {
                    setHos(hos.data.TvEmgcHospitalInfo.row);
                    setLib(lib.data.SeoulPublicLibraryInfo.row);
                    setPark(park.data.SearchParkInfoService.row);
                    setSwim(swim.data.LOCALDATA_103501.row);
                    setGym(gym.data.InnerGmnsmInfo.row);
                    setCol(col.data.SebcCollegeInfoKor.row);
                    setMar(mar.data.ListTraditionalMarket.row);
                    setReh(reh.data.fcltOpenInfo_OMSI.row);
                    setOld(old.data.fcltOpenInfo_OWI.row);
                    setKids(kids.data.tnFcltySttusInfo1011.row);
                })
            )
            .catch((err) => {
                console.log(err);
            });
    }, []);

    // json 파일 데이터들의
    // 폴리곤좌표(positions), 동이름(dongName), isMouseover를 객체 형태로 dongInfo 배열에 저장
    // (동 지도 그리기 위해)
    useEffect(() => {
        if (!map) return;

        const data = geojson.features; // 각 동의 폴리곤 좌표 및 이름 정보가 담긴 배열
        let dongContents = []; // 각 동 정보 객체 넣어줄 배열

        data.forEach((dong) => {
            let coordinates = dong.geometry.coordinates[0]; // 폴리곤 좌표들 담긴 배열

            let content = {}; // dongContents에 넣어줄 각 동 정보 객체
            content.positions = []; // 폴리곤 좌표 value
            content.dongName = dong.properties.EMD_NM; // 동 이름 value
            content.isMouseover = false; // 마우스 오버 이벤트 위한 value

            coordinates.forEach((coord) => {
                // 각 동의 각 폴리곤 좌표를 content.positions에 객체로 형태로 넣어줌
                content.positions.push({
                    lat: coord[1],
                    lng: coord[0],
                });
            });

            dongContents.push(content);
        });

        setDongInfo(dongContents);
    }, [map]);

    // 로딩 창 띄우기
    useEffect(() => {
        if (!map) return;

        if (display === "block") {
            waitBox.current.style.display = "block";
        } else {
            waitBox.current.style.display = "none";
        }
    }, [display]);

    const axiosFunc = async (elem, addrKey, type) => {
        if (elem[addrKey]) {
            let res = await axios.get(`https://dapi.kakao.com/v2/local/search/address.json?query=${elem[addrKey]}`, {
                headers: {
                    Authorization: "KakaoAK c810d1096c3f50297a4f3e8323afece5",
                },
            });

            const location = res.data.documents[0];

            if (location) {
                // console.log("elem확인", elem[name[type]]);
                // console.log("location 확인", location);
                if (location.address) {
                    info.push({
                        name: elem[name[type]],
                        gu: location.address.region_2depth_name,
                        dong: location.address.region_3depth_name,
                        position: {
                            lat: parseFloat(location.y),
                            lng: parseFloat(location.x),
                        },
                    });
                    console.log(info);
                } else {
                    info.push({
                        gu: location.road_address.region_2depth_name,
                        dong: location.road_address.region_3depth_name,
                        lat: location.y,
                        lng: location.x,
                    });
                }
            }
        }
    };

    const arrFunc = (data, addrKey, setFunc, type) => {
        info = [];
        let dataLen = data.length;
        let iLen = Math.floor(dataLen / 100);

        for (let i = 0; i < iLen; i++) {
            for (let j = i * 100; j < (i + 1) * 100; j++) {
                setTimeout(() => axiosFunc(data[j], addrKey, type), (i + 1) * 1000);
            }
        }
        for (let i = iLen * 100; i < dataLen; i++) {
            setTimeout(() => axiosFunc(data[i], addrKey, type), (iLen + 1) * 1000);
        }
        setFunc(info);

        setTimeout(() => setDisplay("none"), 1500);
    };

    const infraBtnClick = (cnt, type) => {
        console.log("Info배열 확인", dataType[type]);
        // cnt가 0인경우 (type이 안골라진 경우)
        if (!cnt) {
            setDisplay("block");

            // 해당Info 에 동정보 담기!!
            // console.log(btn[type][0], btn[type][1], btn[type][2]);
            arrFunc(btn[type][0], btn[type][1], btn[type][2], type);

            // selectedType 배열에 선택한 타입 넣기
            setSelectedType((prevSelectedType) => {
                if (prevSelectedType === []) {
                    return [type];
                } else {
                    return [...prevSelectedType, type];
                }
            });

            // console.log(typeBtn[type].current);
            // 버튼 배경색, 글자색 변경
            typeBtn[type].current.style.backgroundColor = "#756bff";
            typeBtn[type].current.style.color = "white";

            // cnt +1 해주기
            btn[type][3].current++;
        } else {
            // console.log(typeBtn[type].current);
            // cnt가 1인 경우
            // 해당Info 에서 동 정보 빼주기
            btn[type][2]([]);

            // 버튼 배경색, 글자색 변경
            typeBtn[type].current.style.backgroundColor = "white";
            typeBtn[type].current.style.color = "black";

            // cnt 0으로 만들기
            btn[type][3].current--;

            // selectedType 배열에서 type정보 빼주기
            setSelectedType((prevSelectedType) => {
                return prevSelectedType.filter((t) => t !== type);
            });
        }
    };

    // 배열 값 확인
    const click1 = () => {
        console.log("type배열 확인", selectedType);
        console.log("신입 Info 데이터 확인", libInfo);
    };

    const click2 = () => {
        let filteredDong = [];
        if (selectedType) {
            selectedType.forEach((type) => {
                console.log("type foreach 돌아가는지 확인", type, dataType[type]);
                // console.log("donhInfo 배열확인", dongInfo);

                // console.log("확인", dataType[type])

                if (!filteredDong.length) {
                    console.log("if문 들어오기 확인");
                    dataType[type].forEach((infraInfo) => {
                        dongInfo.forEach((dInfo) => {
                            if (infraInfo.dong.includes(dInfo.dongName)) {
                                filteredDong.push(dInfo);
                            }
                        });
                    });
                    filteredDong = [...new Set(filteredDong)];
                    console.log("filteredDong 확인", filteredDong);
                } else {
                    console.log("else들어오기 확인");
                    let temp = [];
                    dataType[type].forEach((infraInfo) => {
                        filteredDong.forEach((dInfo) => {
                            if (infraInfo.dong.includes(dInfo.dongName)) {
                                temp.push(dInfo);
                            }
                        });
                    });
                    temp = [...new Set(temp)];
                    filteredDong = temp;
                }
            });
        }

        console.log("filter 됐는지 확인", filteredDong);
        setSelectedDong(filteredDong);
    };

    // 사이드바 열어주는 함수
    const sideBarOpen = () => {
        menuBtn.current.style.transitionDelay = "0ms";
        sideBar.current.style.transform = "translateX(0)";
        menuBtn.current.style.visibility = "hidden";
    };

    // 사이드바 닫아주는 함수
    const sideBarClose = () => {
        menuBtn.current.style.transitionDelay = "1000ms";
        sideBar.current.style.transform = "translateX(-350px)";
        menuBtn.current.style.visibility = "visible";
    };

    const [isHovering, setIsHovering] = useState(false);

    // 호버문제 해결
    const handleMouseOver = (e, cnt) => {
        if (setIsHovering && cnt === 0) {
            e.target.style.backgroundColor = "#756bff";
            e.target.style.color = "white";
        }
    };

    const handleMouseOut = (e, cnt) => {
        if (setIsHovering && cnt === 0) {
            e.target.style.backgroundColor = "white";
            e.target.style.color = "black";
        }
    };
    return (
        <Layout>
            <MapWrap>
                <SideWrap>
                    <MenuBtn ref={menuBtn} onClick={sideBarOpen}>
                        <CgMenuRound size="30" color="red" />
                    </MenuBtn>
                    <SideBar ref={sideBar}>
                        <CloseBtn onClick={sideBarClose}>
                            <IoMdCloseCircle size="20" />
                        </CloseBtn>
                        <Category>
                            <CateTitle>문화시설</CateTitle>
                            <Infra onMouseOver={(e) => handleMouseOver(e, libClickCnt.current)} onMouseOut={(e) => handleMouseOut(e, libClickCnt.current)} ref={libBtn} onClick={() => infraBtnClick(libClickCnt.current, "library")}>
                                도서관
                            </Infra>
                            <Infra onMouseOver={(e) => handleMouseOver(e, parkClickCnt.current)} onMouseOut={(e) => handleMouseOut(e, parkClickCnt.current)} ref={parkBtn} onClick={() => infraBtnClick(parkClickCnt.current, "park")}>
                                공원
                            </Infra>
                            <Infra onMouseOver={(e) => handleMouseOver(e, marClickCnt.current)} onMouseOut={(e) => handleMouseOut(e, marClickCnt.current)} ref={marBtn} onClick={() => infraBtnClick(marClickCnt.current, "market")}>
                                전통시장
                            </Infra>
                        </Category>
                        <Category>
                            <Infra onMouseOver={(e) => handleMouseOver(e, kidsClickCnt.current)} onMouseOut={(e) => handleMouseOut(e, kidsClickCnt.current)} ref={kidsBtn} onClick={() => infraBtnClick(kidsClickCnt.current, "kids")}>
                                키즈카페
                            </Infra>
                        </Category>
                        <Category>
                            <CateTitle>의료시설</CateTitle>
                            <Infra onMouseOver={(e) => handleMouseOver(e, hosClickCnt.current)} onMouseOut={(e) => handleMouseOut(e, hosClickCnt.current)} ref={hosBtn} onClick={() => infraBtnClick(hosClickCnt.current, "hospital")}>
                                응급실
                            </Infra>
                        </Category>
                        <Category>
                            <CateTitle>체육시설</CateTitle>
                            <Infra onMouseOver={(e) => handleMouseOver(e, gymClickCnt.current)} onMouseOut={(e) => handleMouseOut(e, gymClickCnt.current)} ref={gymBtn} onClick={() => infraBtnClick(gymClickCnt.current, "gym")}>
                                체육관
                            </Infra>
                            <Infra onMouseOver={(e) => handleMouseOver(e, swimClickCnt.current)} onMouseOut={(e) => handleMouseOut(e, swimClickCnt.current)} ref={swimBtn} onClick={() => infraBtnClick(swimClickCnt.current, "swim")}>
                                수영장
                            </Infra>
                        </Category>
                        <Category>
                            <CateTitle>복지시설</CateTitle>
                            <Infra onMouseOver={(e) => handleMouseOver(e, rehClickCnt.current)} onMouseOut={(e) => handleMouseOut(e, rehClickCnt.current)} ref={rehBtn} onClick={() => infraBtnClick(rehClickCnt.current, "reh")}>
                                재활센터
                            </Infra>
                            <Infra onMouseOver={(e) => handleMouseOver(e, oldClickCnt.current)} onMouseOut={(e) => handleMouseOut(e, oldClickCnt.current)} ref={oldBtn} onClick={() => infraBtnClick(oldClickCnt.current, "old")}>
                                노인복지관
                            </Infra>
                        </Category>
                        <Category>
                            <CateTitle>교육시설</CateTitle>
                            <Infra onMouseOver={(e) => handleMouseOver(e, colClickCnt.current)} onMouseOut={(e) => handleMouseOut(e, colClickCnt.current)} ref={colBtn} onClick={() => infraBtnClick(colClickCnt.current, "college")}>
                                대학교
                            </Infra>
                        </Category>
                    </SideBar>
                </SideWrap>

                <Map
                    id={`map`}
                    center={{
                        lat: 37.566826,
                        lng: 126.9786567,
                    }}
                    style={{
                        width: "100%",
                        height: "100vh",
                        position: "absolute",
                    }}
                    level={9}
                    onCreate={setMap}
                    onMouseMove={(_map, mouseEvent) =>
                        setMousePosition({
                            lat: mouseEvent.latLng.getLat(),
                            lng: mouseEvent.latLng.getLng(),
                        })
                    }
                >
                    {selectedDong.map((path, idx) => (
                        <Polygon key={idx} path={path.positions} strokeWeight={2} strokeColor={"red"} strokeOpacity={0.8} strokeStyle={"solid"} fillColor={path.isMouseover ? "pink" : "#ffe6ea"} fillOpacity={0.7} onMouseover={() => (path.isMouseover = true)} onMouseout={() => (path.isMouseover = false)} />
                    ))}
                    {dongInfo.findIndex((v) => v.isMouseover) !== -1 && (
                        <CustomOverlayMap position={mousePosition}>
                            <DivArea>{dongInfo.find((v) => v.isMouseover).dongName}</DivArea>
                        </CustomOverlayMap>
                    )}

                    {libInfo.map((row, idx) => (
                        <MapMarker
                            key={idx}
                            position={row.position}
                            title={row.name}
                            image={{
                                src: "/library_marker.png",
                                size: {
                                    width: 30,
                                    height: 35,
                                },
                            }}
                        ></MapMarker>
                    ))}
                    {parkInfo.map((row, idx) => (
                        <MapMarker
                            key={idx}
                            position={row.position}
                            title={row.name}
                            image={{
                                src: "/park_marker.png",
                                size: {
                                    width: 31,
                                    height: 35,
                                },
                            }}
                        ></MapMarker>
                    ))}
                    {marInfo.map((row, idx) => (
                        <MapMarker
                            key={idx}
                            position={row.position}
                            title={row.name}
                            image={{
                                src: "/market_marker.png",
                                size: {
                                    width: 31,
                                    height: 35,
                                },
                            }}
                        ></MapMarker>
                    ))}
                    {kidsInfo.map((row, idx) => (
                        <MapMarker
                            key={idx}
                            position={row.position}
                            title={row.name}
                            image={{
                                src: "/kids_marker.png",
                                size: {
                                    width: 31,
                                    height: 35,
                                },
                            }}
                        ></MapMarker>
                    ))}
                    {hosInfo.map((row, idx) => (
                        <MapMarker
                            key={idx}
                            position={row.position}
                            title={row.name}
                            image={{
                                src: "/hospital_marker.png",
                                size: {
                                    width: 31,
                                    height: 35,
                                },
                            }}
                        ></MapMarker>
                    ))}
                    {gymInfo.map((row, idx) => (
                        <MapMarker
                            key={idx}
                            position={row.position}
                            title={row.name}
                            image={{
                                src: "/gym_marker.png",
                                size: {
                                    width: 31,
                                    height: 35,
                                },
                            }}
                        ></MapMarker>
                    ))}
                    {swimInfo.map((row, idx) => (
                        <MapMarker
                            key={idx}
                            position={row.position}
                            title={row.name}
                            image={{
                                src: "/swim_marker.png",
                                size: {
                                    width: 31,
                                    height: 35,
                                },
                            }}
                        ></MapMarker>
                    ))}
                    {rehInfo.map((row, idx) => (
                        <MapMarker
                            key={idx}
                            position={row.position}
                            title={row.name}
                            image={{
                                src: "/reh_marker.png",
                                size: {
                                    width: 31,
                                    height: 35,
                                },
                            }}
                        ></MapMarker>
                    ))}
                    {oldInfo.map((row, idx) => (
                        <MapMarker
                            key={idx}
                            position={row.position}
                            title={row.name}
                            image={{
                                src: "/old_marker.png",
                                size: {
                                    width: 31,
                                    height: 35,
                                },
                            }}
                        ></MapMarker>
                    ))}
                    {colInfo.map((row, idx) => (
                        <MapMarker
                            key={idx}
                            position={row.position}
                            title={row.name}
                            image={{
                                src: "/college_marker.png",
                                size: {
                                    width: 31,
                                    height: 35,
                                },
                            }}
                        ></MapMarker>
                    ))}
                </Map>
            </MapWrap>

            <WaitBox ref={waitBox}>
                <Spin tip="Loading">
                    <div />
                </Spin>
            </WaitBox>
        </Layout>
    );
};

export default map;