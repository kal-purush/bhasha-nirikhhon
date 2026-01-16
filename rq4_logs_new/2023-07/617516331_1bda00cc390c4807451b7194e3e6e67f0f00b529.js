import Admin from '@/src/pages/admin/template';
import { getCurrentEnvironment } from '@/src/utils';
const AdminPage = () => {
  const currentEnvironment = getCurrentEnvironment();
  if (currentEnvironment != 'development') return null;
  return <Admin />;
};

export default AdminPage;