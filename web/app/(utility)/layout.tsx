"use client";

import { usePathname } from "next/navigation";
import UtilitySidebar from "@/components/sidebar/UtilitySidebar";

export default function UtilityLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const pathname = usePathname();
  return (
    <div className="flex h-screen overflow-hidden">
      <UtilitySidebar />
      <main className="flex-1 overflow-hidden bg-[var(--background)]">
        {/* key={pathname} forces React to treat each route as a clean
            unmount/remount instead of trying to reconcile the old page's
            DOM nodes with the new page's tree. This avoids React 19
            concurrent-mode insertBefore race in commitPlacement. */}
        <div key={pathname}>{children}</div>
      </main>
    </div>
  );
}
