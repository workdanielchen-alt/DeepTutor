import { Loader2 } from "lucide-react";

export default function UtilityLoading() {
  return (
    <div className="flex h-screen items-center justify-center bg-[var(--background)]">
      <Loader2 className="h-5 w-5 animate-spin text-[var(--muted-foreground)]" />
    </div>
  );
}
