/**
 * Knowledge base page.
 *
 * Intentionally server-rendered without a Suspense boundary so React 19
 * never needs to interleave concurrent Suspense resolution with route
 * transitions — this avoids the commitPlacement insertBefore crash
 * (Space → Knowledge → Space). The inner <KnowledgePage /> client
 * component handles its own deferred loading state.
 */
import KnowledgePage from "@/components/knowledge/KnowledgePage";

export const dynamic = "force-dynamic";

export default function Page() {
  return <KnowledgePage />;
}
