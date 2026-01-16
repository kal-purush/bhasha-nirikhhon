export default ({ store, redirect }) => {
  if(!store.state.authorized) {
    redirect('/login');
  } else {
    redirect('/');
  }
};