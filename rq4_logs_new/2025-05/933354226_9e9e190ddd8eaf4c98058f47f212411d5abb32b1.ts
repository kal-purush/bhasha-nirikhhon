export interface AdminDashboardResponse {
  users: {
    total: number;
    instructors: number;
    students: number;
    recentSignups: {
      fullName: string;
      email: string;
      createdAt: string;
    }[];
  };
  courses: {
    total: number;
    published: number;
    draft: number;
    paid: number;
    free: number;
  };
  payments: {
    totalOrders: number;
    totalRevenue: number;
    paid: number;
    pending: number;
    failed: number;
    recentOrders: {
      user: {
        fullName: string;
        email: string;
      };
      courses: {
        title: string;
        price: number;
      }[];
      createdAt: string; // ISO date string
    }[];
  };
  enrollments: {
    total: number;
    topCourses: {
      title: string;
      count: number;
    }[];
  };
  metadata: {
    totalCategories: number;
    totalTags: number;
  };
}