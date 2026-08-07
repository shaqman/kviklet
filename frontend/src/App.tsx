import { lazy, ReactElement, Suspense, useContext } from "react";
import { Navigate, Route, Routes } from "react-router-dom";
import RootLayout from "./layout/RootLayout";
import Login from "./routes/Login";
import {
  UserStatusContext,
  UserStatusProvider,
} from "./components/UserStatusProvider";
import { ThemeStatusProvider } from "./components/ThemeStatusProvider";
import { NotificationContextProvider } from "./components/NotifcationStatusProvider";
import { ConfigProvider } from "./components/ConfigProvider";

const Settings = lazy(() => import("./routes/settings/Settings"));
const Requests = lazy(() =>
  import("./routes/Requests").then(({ Requests }) => ({ default: Requests })),
);
const ConnectionChooser = lazy(() => import("./routes/NewRequest"));
const Auditlog = lazy(() => import("./routes/Auditlog"));
const RequestReview = lazy(() => import("./routes/Review"));
const LiveSessionWebsockets = lazy(
  () => import("./routes/LiveSessionWebsockets"),
);

const RouteLoading = () => <div>Loading...</div>;

export interface ProtectedRouteProps {
  children: ReactElement;
}

export const ProtectedRoute = ({
  children,
}: ProtectedRouteProps): ReactElement => {
  const userContext = useContext(UserStatusContext);

  if (userContext.userStatus === undefined) {
    return <div>Loading...</div>;
  }
  if (userContext.userStatus === false) {
    return <Navigate to="/login" />;
  }
  return children;
};
function App() {
  return (
    <div className="min-h-screen bg-slate-50 text-slate-900 transition-colors dark:bg-slate-950 dark:text-slate-50">
      <UserStatusProvider>
        <ThemeStatusProvider>
          <NotificationContextProvider>
            <ConfigProvider>
              <Suspense fallback={<RouteLoading />}>
                <Routes>
                  <Route path="/" element={<RootLayout />}>
                    <Route
                      index
                      element={
                        <ProtectedRoute>
                          <Requests />
                        </ProtectedRoute>
                      }
                    />
                    <Route
                      path="settings/*"
                      element={
                        <ProtectedRoute>
                          <Settings />
                        </ProtectedRoute>
                      }
                    />
                    <Route
                      path="new"
                      element={
                        <ProtectedRoute>
                          <ConnectionChooser />
                        </ProtectedRoute>
                      }
                    />
                    <Route
                      path="requests"
                      element={
                        <ProtectedRoute>
                          <Requests />
                        </ProtectedRoute>
                      }
                    />
                    <Route
                      path="auditlog"
                      element={
                        <ProtectedRoute>
                          <Auditlog />
                        </ProtectedRoute>
                      }
                    />
                    <Route
                      path="requests/:requestId"
                      element={
                        <ProtectedRoute>
                          <RequestReview />
                        </ProtectedRoute>
                      }
                    />
                    <Route
                      path="requests/:requestId/session"
                      element={
                        <ProtectedRoute>
                          <LiveSessionWebsockets />
                        </ProtectedRoute>
                      }
                    />
                    <Route path="login" element={<Login />} />
                  </Route>
                </Routes>
              </Suspense>
            </ConfigProvider>
          </NotificationContextProvider>
        </ThemeStatusProvider>
      </UserStatusProvider>
    </div>
  );
}

export default App;
