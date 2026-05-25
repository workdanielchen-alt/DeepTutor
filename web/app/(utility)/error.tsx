"use client";

export default function UtilityError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <div className="flex h-screen flex-col items-center justify-center gap-4 px-6 text-center">
      <div className="text-[15px] font-medium text-[var(--muted-foreground)]">
        Something went wrong
      </div>
      <button
        onClick={() => reset()}
        className="rounded-lg border border-[var(--border)] px-4 py-2 text-[13px] font-medium text-[var(--foreground)] transition-colors hover:bg-[var(--muted)]"
      >
        Try again
      </button>
    </div>
  );
}
