"use client";

import { useEffect, useState } from "react";

/**
 * Catches React 19 concurrent-mode commitPlacement errors (NotFoundError:
 * insertBefore) that occur during route transitions between sibling pages
 * in the (utility) group.  Instead of showing an error screen we
 * auto-recover via reset() — the user sees a brief blank state while
 * React remounts the tree.
 */
function isInsertBeforeError(error: Error) {
  return (
    error.name === "NotFoundError" &&
    error.message.includes("insertBefore")
  );
}

interface UtilityErrorProps {
  error: Error & { digest?: string };
  reset: () => void;
}

export default function UtilityError({ error, reset }: UtilityErrorProps) {
  const [recovering, setRecovering] = useState(false);
  const silent = isInsertBeforeError(error);

  useEffect(() => {
    if (silent && !recovering) {
      setRecovering(true);
      // Yield one frame so React finishes tearing down the broken tree,
      // then reset to remount cleanly.
      requestAnimationFrame(() => reset());
    }
  }, [silent, recovering, reset]);

  // insertBefore errors: show nothing while recovering
  if (silent) return null;

  // All other errors: show actionable UI
  return (
    <div className="flex h-screen flex-col items-center justify-center gap-4 px-6 text-center">
      <div className="text-[15px] font-medium text-[var(--muted-foreground)]">
        Something went wrong
      </div>
      <pre className="max-w-lg overflow-auto text-left text-[11px] text-[var(--muted-foreground)]/70">
        {error instanceof Error
          ? `${error.name}: ${error.message}${error.digest ? `\ndigest: ${error.digest}` : ""}\n${error.stack?.split("\n").slice(0, 4).join("\n") ?? ""}`
          : JSON.stringify(error, null, 2)}
      </pre>
      <button
        onClick={() => reset()}
        className="rounded-lg border border-[var(--border)] px-4 py-2 text-[13px] font-medium text-[var(--foreground)] transition-colors hover:bg-[var(--muted)]"
      >
        Try again
      </button>
    </div>
  );
}
