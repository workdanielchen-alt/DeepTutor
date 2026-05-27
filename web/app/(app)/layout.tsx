"use client";

import { usePathname } from "next/navigation";
import WorkspaceSidebar from "@/components/sidebar/WorkspaceSidebar";
import UtilitySidebar from "@/components/sidebar/UtilitySidebar";
import { UnifiedChatProvider } from "@/context/UnifiedChatContext";

const WORKSPACE_PATHS = ["/", "/chat", "/agents", "/book", "/playground"];

export default function AppLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const pathname = usePathname();
  const isWorkspace = WORKSPACE_PATHS.some((p) => pathname === p || pathname.startsWith(p + "/"));

  return (
    <UnifiedChatProvider>
      <div className="flex h-screen overflow-hidden">
        {isWorkspace ? <WorkspaceSidebar /> : <UtilitySidebar />}
        <main className="flex-1 overflow-hidden bg-[var(--background)]">
          {children}
        </main>
      </div>
    </UnifiedChatProvider>
  );
}
