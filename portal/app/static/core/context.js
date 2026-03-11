import { api } from "./api.js";
import { toast } from "../components/Toast.js";


export const AppCtx = React.createContext(null);

const TOKEN_STORAGE_KEY = "ecopad_token";


export function AppProvider({ children, navigate }) {
  const [auth, setAuth] = React.useState({
    status: "loading",
    user: null,
    token: null,
  });

  // -------------------------------------------------------------------
  // Restore session from localStorage
  // -------------------------------------------------------------------
  React.useEffect(() => {
    const token = localStorage.getItem(TOKEN_STORAGE_KEY);

    if (!token) {
      setAuth({
        status: "ready",
        user: null,
        token: null,
      });
      return;
    }

    api.me(token)
      .then((result) => {
        if (!result?.user) {
          localStorage.removeItem(TOKEN_STORAGE_KEY);
          setAuth({
            status: "ready",
            user: null,
            token: null,
          });
          return;
        }

        setAuth({
          status: "ready",
          user: result.user,
          token,
        });
      })
      .catch(() => {
        localStorage.removeItem(TOKEN_STORAGE_KEY);
        setAuth({
          status: "ready",
          user: null,
          token: null,
        });
      });
  }, []);

  // -------------------------------------------------------------------
  // Auth actions
  // -------------------------------------------------------------------
  async function login(username, password) {
    const result = await api.login(username, password);

    localStorage.setItem(TOKEN_STORAGE_KEY, result.token);

    setAuth({
      status: "ready",
      user: result.user,
      token: result.token,
    });

    toast("Logged in");
    if (navigate) navigate("Home");
  }

  async function register(username, password) {
    await api.register(username, password);
    toast("Registered. Please log in.");
  }

  async function logout() {
    try {
      if (auth.token) {
        await api.logout(auth.token);
      }
    } catch {
      // ignore logout network errors
    }

    localStorage.removeItem(TOKEN_STORAGE_KEY);

    setAuth({
      status: "ready",
      user: null,
      token: null,
    });

    toast("Logged out");
    if (navigate) navigate("Login");
  }

  const value = React.useMemo(() => ({
    auth,
    setAuth,
    navigate,
    login,
    register,
    logout,
  }), [auth, navigate]);

  return React.createElement(AppCtx.Provider, { value }, children);
}