export interface MenuAttribute {
  path: string; // 菜单路径
  id: number; // 标识 id
  parentId: number; // 父 id 0: 根项目
  type: string; // 菜单类型 page: 全屏切换页面; layout: 有公共布局; layoutMenu: 公共布局内的菜单页面
  layoutComponent: string; // 如果是公共布局页面，使用的哪个公共布局组件 Layout
  label: string; // 文字标签
  component: {
    // 组件信息
    name: string; // 组件标签名称
    file: string; // 组件文件地址，此地址在 /src 目录下
    icon?: string; // 图标
    permissionType?: string; // 权限类型 content: 页面内容; button: 按钮
  };
  children?: MenuAttribute[]; // 子路由
}

export const accountRouter: MenuAttribute[] = [
  // 登录
  {
    path: "/login",
    id: 1, // id
    parentId: 0,
    type: "page",
    label: "登录",
    layoutComponent: "",
    component: {
      name: "Login",
      file: "/views/login/Login",
      icon: "",
      permissionType: "content",
    },
  },
  // 首页
  {
    path: "/home",
    id: 2,
    parentId: 0,
    type: "layout",
    layoutComponent: "Layout",
    label: "首页",
    component: {
      name: "Layout",
      file: "/components/layout/Layout",
    },
    children: [
      // 首页
      {
        path: "all",
        id: 21,
        parentId: 2,
        type: "layoutMenu",
        layoutComponent: "",
        label: "首页",
        component: {
          name: "Home",
          file: "/views/admin/home/Home",
          icon: "",
          permissionType: "content",
        },
      },
    ],
  },
  // 用户管理
  {
    path: "/user",
    id: 3,
    parentId: 0,
    type: "layout",
    layoutComponent: "Layout",
    label: "用户管理",
    component: {
      name: "Layout",
      file: "/components/layout/Layout",
    },
    children: [
      {
        path: "all_list",
        id: 31,
        parentId: 3,
        type: "layoutMenu",
        layoutComponent: "",
        label: "用户列表",
        component: {
          name: "UserList",
          file: "/views/admin/user/user_list/UserList",
          icon: "",
          permissionType: "content",
        },
      },
    ],
  },
  // 权限管理
  {
    path: "/authority",
    id: 4,
    parentId: 0,
    type: "layout",
    layoutComponent: "Layout",
    label: "权限管理",
    component: {
      name: "Layout",
      file: "/components/layout/Layout",
    },
    children: [
      {
        path: "role",
        id: 41,
        parentId: 4,
        type: "layoutMenu",
        layoutComponent: "",
        label: "角色列表",
        component: {
          name: "Role",
          file: "/views/admin/authority/role/Role",
          icon: "",
          permissionType: "content",
        },
      },
      {
        path: "permission",
        id: 42,
        parentId: 4,
        type: "layoutMenu",
        layoutComponent: "",
        label: "权限列表",
        component: {
          name: "Permission",
          file: "/views/admin/authority/permission/Permission",
          icon: "",
          permissionType: "content",
        },
      },
    ],
  },
  // 新闻管理
  {
    path: "/news",
    id: 5,
    parentId: 0,
    type: "layout",
    layoutComponent: "Layout",
    label: "新闻管理",
    component: {
      name: "Layout",
      file: "/components/layout/Layout",
    },
    children: [
      {
        path: "edit",
        id: 51,
        parentId: 5,
        type: "layoutMenu",
        layoutComponent: "",
        label: "撰写新闻",
        component: {
          name: "Edit",
          file: "/views/admin/news/edit/Edit",
          icon: "",
          permissionType: "content",
        },
      },
      {
        path: "draft",
        id: 52,
        parentId: 5,
        type: "layoutMenu",
        layoutComponent: "",
        label: "草稿箱",
        component: {
          name: "Draft",
          file: "/views/admin/news/draft/Draft",
          icon: "",
          permissionType: "content",
        },
      },
      {
        path: "classify",
        id: 53,
        parentId: 5,
        type: "layoutMenu",
        layoutComponent: "",
        label: "新闻分类",
        component: {
          name: "Classify",
          file: "/views/admin/news/classify/Classify",
          icon: "",
          permissionType: "content",
        },
      },
    ],
  },
  // 审核管理
  {
    path: "/check",
    id: 6,
    parentId: 0,
    type: "layout",
    layoutComponent: "Layout",
    label: "审核管理",
    component: {
      name: "Layout",
      file: "/components/layout/Layout",
    },
    children: [
      {
        path: "news",
        id: 61,
        parentId: 6,
        type: "layoutMenu",
        layoutComponent: "",
        label: "审核新闻",
        component: {
          name: "CheckNews",
          file: "/views/admin/check/check_news/CheckNews",
          icon: "",
          permissionType: "content",
        },
      },
      {
        path: "list",
        id: 62,
        parentId: 6,
        type: "layoutMenu",
        layoutComponent: "",
        label: "审核列表",
        component: {
          name: "CheckList",
          file: "/views/admin/check/check_list/CheckList",
          icon: "",
          permissionType: "content",
        },
      },
    ],
  },
  // 发布管理
  {
    path: "/publish",
    id: 7,
    parentId: 0,
    type: "layout",
    layoutComponent: "Layout",
    label: "发布管理",
    component: {
      name: "Layout",
      file: "/components/layout/Layout",
    },
    children: [
      {
        path: "await",
        id: 71,
        parentId: 7,
        type: "layoutMenu",
        layoutComponent: "",
        label: "待发布",
        component: {
          name: "PublishAwait",
          file: "/views/admin/publish/await/Await",
          icon: "",
          permissionType: "content",
        },
      },
      {
        path: "done",
        id: 72,
        parentId: 7,
        type: "layoutMenu",
        layoutComponent: "",
        label: "已发布",
        component: {
          name: "PublishDone",
          file: "/views/admin/publish/done/Done",
          icon: "",
          permissionType: "content",
        },
      },
      {
        path: "offline",
        id: 73,
        parentId: 7,
        type: "layoutMenu",
        layoutComponent: "",
        label: "已下线",
        component: {
          name: "Offline",
          file: "/views/admin/publish/offline/Offline",
          icon: "",
          permissionType: "content",
        },
      },
    ],
  },
  // 模糊匹配
  {
    path: "*",
    id: 8,
    parentId: 0,
    type: "page",
    layoutComponent: "",
    label: "模糊匹配",
    component: {
      name: "Skeleton",
      file: "/views/helper/skeleton/Skeleton",
      icon: "",
      permissionType: "content",
    },
  },
] 