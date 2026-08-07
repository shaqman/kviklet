import React, { useCallback, useEffect, useRef, useState } from "react";
import { StatusResponse, checklogin } from "../api/StatusApi";

type UserContext = {
  userStatus: StatusResponse | false | undefined;
  refreshState: () => Promise<void>;
};

const UserStatusContext = React.createContext<UserContext>({
  userStatus: undefined,
  refreshState: async () => {},
});

type Props = {
  children: React.ReactNode;
};

export const UserStatusProvider: React.FC<Props> = ({ children }) => {
  const [userStatus, setUserStatus] = useState<
    StatusResponse | false | undefined
  >(undefined);
  const statusRequest = useRef<Promise<void> | null>(null);

  const fetchStatus = useCallback(async (): Promise<void> => {
    if (statusRequest.current) {
      return statusRequest.current;
    }

    const request = checklogin()
      .then((status) => {
        setUserStatus(status);
      })
      .catch((error) => {
        console.error("Failed to fetch user status:", error);
      })
      .finally(() => {
        statusRequest.current = null;
      });

    statusRequest.current = request;
    return request;
  }, []);

  const handleVisibilityChange = useCallback(() => {
    if (document.visibilityState === "visible") {
      void fetchStatus();
    }
  }, [fetchStatus]);

  useEffect(() => {
    void fetchStatus();
  }, [fetchStatus]);

  useEffect(() => {
    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () => {
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [handleVisibilityChange]);

  return (
    <UserStatusContext.Provider
      value={{ userStatus, refreshState: fetchStatus }}
    >
      {children}
    </UserStatusContext.Provider>
  );
};

export { UserStatusContext };
