import { initializeApp } from "firebase/app";
import { v4 as uuid } from "uuid";
import { getDatabase, ref, get, set } from "firebase/database";
import {
  getAuth,
  signInWithPopup,
  GoogleAuthProvider,
  signOut,
  onAuthStateChanged,
} from "firebase/auth";
import { banner, userData } from "../types/type";

// import { getAnalytics } from "firebase/analytics";

const firebaseConfig = {
  apiKey: import.meta.env.VITE_FIREBASE_API_KEY,
  authDomain: import.meta.env.VITE_FIREBASE_AUTH_DOMAIN,
  databaseURL: import.meta.env.VITE_FIREBASE_DATABASE_URL,
  projectId: import.meta.env.VITE_FIREBASE_PROJECT_ID,
};

const app = initializeApp(firebaseConfig);
const auth = getAuth();
const provider = new GoogleAuthProvider();
const database = getDatabase(app);
// const analytics = getAnalytics(app);

//유저관련
export async function userLogin() {
  try {
    const res = await signInWithPopup(auth, provider);
    const user = res.user;

    const dataSnapshot = await get(ref(database, "userTest"));
    const data = dataSnapshot.val();

    const foundObject = Object.keys(data).find((uid) => uid === user.uid);

    if (foundObject) {
      console.log("이미 가입한 유저입니다.");
      // API 동작 멈추기
    } else {
      const currentDate = new Date();
      const year = currentDate.getFullYear();
      const month = String(currentDate.getMonth() + 1).padStart(2, "0");
      const day = String(currentDate.getDate()).padStart(2, "0");

      const formattedDate = `${year}-${month}-${day}`;

      const newUserData = {
        uid: user.uid,
        img_url: user.photoURL,
        email: user.email,
        name: user.displayName,
        nick_name: user.displayName,
        registration_date: formattedDate,
        ban: { ban_content: "", ban_end_date: "", ban_state_date: "", ban_type: "" },
        delete: { delete_content: "", delete_State: "N", delete_at: "" },
        role: "user",
        state: "user",
      };
      console.log("추가합니다");

      console.log(newUserData);

      await set(ref(database, "userTest/" + user.uid), newUserData);
    }
  } catch (error) {
    console.error("로그인 또는 데이터베이스 작업에 실패했습니다.", error);
  }
}

export function userLogOut() {
  console.log("logOut");
  signOut(auth);
}

export function onUserStateChanged(callback: any) {
  onAuthStateChanged(auth, (user) => {
    const updatedUser = user ? user : null;
    console.log(updatedUser);
    callback(updatedUser);
  });
}

//어드민 관련
export function adminLogin() {
  signInWithPopup(auth, provider) //
    .catch(console.error);
}
export function adminLogOut() {
  signOut(auth);
}

export function onAdminStateChanged(callback: any) {
  onAuthStateChanged(auth, async (user) => {
    const updatedUser = user ? await adminUser(user) : null;
    callback(updatedUser);
  });
}

async function adminUser(user: any) {
  return get(ref(database, "admins")) //
    .then((snapshot) => {
      if (snapshot.exists()) {
        const admins = snapshot.val();
        const isAdmin = admins.includes(user.uid);
        return { ...user, isAdmin };
      }
      return user;
    });
}

//이미지 등록
export async function addBannerImage(bannerData: any, imgUrl: any) {
  return get(ref(database, "bannerItem")).then((res) => {
    const data = res.val();
    console.log(data);
    if (!data) {
      set(ref(database, "bannerItem"), "");
    }

    const id = Object.keys(data).length + 1;

    console.log(id);

    const newData = {
      ...bannerData,
      id: `banner-${id}`,
      url: imgUrl,
    };

    set(ref(database, `bannerItem/${bannerData.title}`), newData);
    return newData;
  });
}
export async function getBannerList() {
  const res = await get(ref(database, "banner"));
  return res.val();
}

export async function getBannerItemList() {
  const res = await get(ref(database, "bannerItem"));
  return res.val();
}

// export function getBannerList() {
//   return get(ref(database, "banner")).then((res) => res.val());
// }

// export function getBannerItemList() {
//   return get(ref(database, "bannerItem")).then((res) => res.val());
// }

export function addBanner(bannerList: any) {
  get(ref(database, "bannerItem")).then((res) => {
    const data = res.val();
    if (!data) {
      set(ref(database, "bannerItem"), "");
    } else {
      set(ref(database, "banner"), { ...bannerList });
    }
  });
}

export function deleteBanner(id: string) {
  return get(ref(database, "bannerItem")).then((res) => {
    const data = Object.values(res.val());
    const newData = data.filter((user) => user.id !== id);
    set(ref(database, "bannerItem"), newData);
    return newData;
  });
}

export function itemDelete(url: string) {
  return get(ref(database, "banner")).then((res) => {
    console.log(url);
    const data = Object.values(res.val());
    const newData = data.filter((item) => item.url !== url);
    set(ref(database, "banner"), newData);
    return newData;
  });
}

//report
export function getReportStateAll() {
  return get(ref(database, "report")).then((res) => {
    const data = res.val();
    return Object.values(data);
  });
}

export function getReportStateTrue() {
  return get(ref(database, "report")) //
    .then((res) => {
      const data = res.val();
      return Object.values(data).filter((user) => user.state === true);
    });
}
export function getReportStateFalse() {
  return get(ref(database, "report")) //
    .then((res) => {
      const data = res.val();
      return Object.values(data).filter((user) => user.state === false);
    });
}

export function searchReportUser(userName: string) {
  get(ref(database, "report")) //
    .then((res) => {
      const data = res.val();
      return Object.values(data).filter((user) => user.name.includes(userName));
    });
}

export function reportChecked(userName: string) {
  get(ref(database, "report")).then((res) => {
    const data = res.val();
    Object.values(data).forEach((user) => {
      if (user.name === userName) {
        user.state = true;
        alert("변경되었습니다.");
      }
    });
    set(ref(database, "report"), data); // 변경된 데이터를 데이터베이스에 적용합니다.
    return data;
  });
}

export function reportDelete(id: number) {
  return get(ref(database, "report")).then((res) => {
    const data = res.val();
    const newData = Object.values(data).filter((user) => user.id !== id);
    console.log(newData);

    const message = confirm("해당 신고내용을 삭제하시겠습니까?");
    if (message) {
      set(ref(database, "report"), newData); // 변경된 데이터를 데이터베이스에 적용합니다.
      alert("삭제되었습니다.");
    } else {
      alert("취소되었습니다.");
    }

    console.log(newData);

    return newData;
  });
}

//회원관리
//전체 유저 리스트
export function getUserListAll() {
  return get(ref(database, "users")).then((res) => {
    const data = res.val();
    console.log(data);
    return Object.values(data);
  });
}

//일반 유저 리스트 (탈퇴x)
export function getUserList() {
  return get(ref(database, "users")) //
    .then((res) => {
      const data = res.val();
      return Object.values(data).filter(
        (user) => user.state === "user" && user.delete.delete_state === "N"
      );
    });
}

//정지 유저 (ban)
export function getSuspendedUser() {
  return get(ref(database, "users")) //
    .then((res) => {
      const data = res.val();
      return Object.values(data).filter((user) => user.state === "ban");
    });
}
//탈퇴 유저
export function getDeleteUser() {
  return get(ref(database, "users")) //
    .then((res) => {
      const data = res.val();
      return Object.values(data).filter((user) => user.delete.delete_state === "Y");
    });
}

export function searchUser(userName: string) {
  return get(ref(database, "users")) //
    .then((res) => {
      const data = res.val();
      return Object.values(data).filter((user) => user.name.includes(userName));
    });
}

//회원 정지
export function userSuspend(
  userName: string,
  banType: string,
  content: string,
  startDate?: string,
  endDate?: string
) {
  console.log(banType);

  return get(ref(database, "users")) //
    .then((res) => {
      const data = res.val();
      const newData = Object.values(data) as userData[];
      newData.forEach((user) => {
        if (user.name === userName) {
          user.state = "ban";
          user.ban.ban_type = banType;
          user.ban.ban_content = content;
          if (banType === "temporary" && startDate !== undefined && endDate !== undefined) {
            user.ban.ban_start_date = startDate;
            user.ban.ban_end_date = endDate;
          } else if (banType === "permanent") {
            const currentDate = new Date();

            const year = currentDate.getFullYear();
            const month = String(currentDate.getMonth() + 1).padStart(2, "0");
            const day = String(currentDate.getDate()).padStart(2, "0");

            const formattedDate = `${year}-${month}-${day}`;
            console.log(formattedDate);
            user.ban.ban_start_date = formattedDate;
            user.ban.ban_end_date = "";
          }
          alert("변경되었습니다.");
        }
      });
      console.log(newData);
      set(ref(database, "users"), newData);
      return newData;
    });
}
//회원 정지 취소
export function userUnsuspend(userName: string) {
  return get(ref(database, "users")) //
    .then((res) => {
      const data = res.val();
      const newData = Object.values(data) as userData[];
      newData.forEach((user) => {
        if (user.name === userName) {
          if (user.state !== "ban") {
            alert("정지 상태의 유저가 아닙니다.");
            return;
          } else {
            user.state = "user";
            user.ban.ban_content = "";
            user.ban.ban_start_date = "";
            user.ban.ban_end_date = "";
            user.ban.ban_type = "";
            alert("변경되었습니다.");
          }
        }
      });
      console.log(newData);
      set(ref(database, "users"), newData);
      return newData;
    });
}

//=================

//banner

export async function getMainBanner() {
  const bannerList = Object.values(await getBannerList());
  const bannerItemList = Object.values(await getBannerItemList());

  const bannerArr = {};
  const bannerItemArr = {};

  for (const item of bannerList) {
    bannerArr[item.id] = item;
  }
  for (const item of bannerItemList) {
    bannerItemArr[item.id] = item;
  }

  const newData = [];
  for (const id in bannerArr) {
    if (bannerItemArr[id]) {
      const combined = {
        ...bannerItemArr[id],
      };
      newData.push(combined);
    }
  }

  // console.log(newData);

  return newData;
}