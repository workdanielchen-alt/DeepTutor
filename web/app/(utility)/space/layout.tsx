import SpaceMiniNav from "@/components/space/SpaceMiniNav";

export default function SpaceLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <div className="flex h-full overflow-hidden">
      <SpaceMiniNav />
      <div className="flex min-w-0 flex-1 flex-col overflow-hidden bg-[var(--background)]">
        <div className="min-h-0 flex-1 overflow-y-auto [scrollbar-gutter:stable]">
          <div className="mx-auto max-w-5xl px-8 py-8 pb-12">{children}</div>
        </div>
      </div>
    </div>
  );
}
