import mapleitem_logo from "../img/project_logo_mapleitem.png";
import army_logo from "../img/project_logo_army.png";
import starrynight_logo from "../img/project_logo_starrynight.png";
import React from "react";

function project() {
    return (
        <section_white id="project">
            <p className="subtitle">Projects</p>
            <div>
                <table>
                    <tr>
                        <td>
                            2020. 01
                        </td>
                        <td>
                            <img
                                className='rounded-circle'
                                src={mapleitem_logo}
                                width='120'
                                alt='mapleitem_logo'
                            />
                        </td>
                        <td>
                            <p className="tableTitle">메이플 추가옵션 분석기</p>
                            <ul>
                                <li>- 웹 사이트 (현재 서비스 중)</li>
                                <li>- 온라인게임 '메이플스토리'의 아이템 가치를 분석합니다.</li>
                                <li>- <a href="http://mapleitem.com" target="_blank">service url: mapleitem.com</a></li>
                                <li>
                                    <span className="detailSkill">Node.js</span>
                                    <span className="detailSkill">express</span>
                                    <span className="detailSkill">bootstrap</span>
                                    <span className="detailSkill">JavaScript</span>
                                </li>
                            </ul>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            2019. 07
                        </td>
                        <td>
                            <img
                                className='rounded-circle'
                                src={army_logo}
                                width='120'
                                alt='army_logo'
                            />
                        </td>
                        <td>
                            <p className="tableTitle">Army Q&A</p>
                            <ul>
                                <li>- 웹 사이트</li>
                                <li>- 군 생활에 필요한 정보를 묻고, 답할 수 있는 페이지입니다.</li>
                                <li>- 2019 국방부오픈소스아카데미 참여 프로젝트</li>
                                <li>- <a href="https://github.com/kyr9389/OSSCAMP_Q-A" target="_blank">Github link</a></li>
                                <li>
                                    <span className="detailSkill">Node.js</span>
                                    <span className="detailSkill">Vue.js</span>
                                    <span className="detailSkill">element.ui</span>
                                    <span className="detailSkill">JavaScript</span>
                                </li>
                            </ul>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            2017. 10
                        </td>
                        <td>
                            <img
                                className='rounded-circle'
                                src={starrynight_logo}
                                width='120'
                                alt='starrynight_logo'
                            />
                        </td>
                        <td>
                            <p className="tableTitle">Starry Night</p>
                            <ul>
                                <li>- 안드로이드 어플리케이션</li>
                                <li>- 오늘 하루를 기록하고, 소통할 수 있는 커뮤니티 앱입니다.</li>
                                <li>- DB 서버 만료로 이용불가</li>
                                <li>- <a href="https://github.com/kyr9389/Starry_night" target="_blank">Github link</a></li>
                                <li>
                                    <span className="detailSkill">Android</span>
                                    <span className="detailSkill">MySQL</span>
                                    <span className="detailSkill">JAVA</span>
                                </li>
                            </ul>
                        </td>
                    </tr>
                </table>
            </div>

        </section_white>
    )
}

export default project;