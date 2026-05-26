import ChatHistorySection from "@/components/space/ChatHistorySection";

/** Render chat history directly at /space to avoid DOM reconciliation
 *  errors caused by server-side redirect during client-side navigation
 *  (Space → Knowledge → Space triggers insertBefore DOM error). */
export default function SpaceIndexPage() {
  return <ChatHistorySection />;
}
