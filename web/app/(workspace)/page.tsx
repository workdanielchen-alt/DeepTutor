"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

/**
 * Root page now redirects to /space/learning (learning dashboard).
 * Handles backward compatibility for /?session=xxx URLs.
 */
export default function HomePage() {
  const router = useRouter();

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const sessionId = params.get("session");

    if (sessionId) {
      router.replace(`/chat/${sessionId}`);
    } else {
      router.replace("/space/learning");
    }
  }, [router]);

  return null;
}
