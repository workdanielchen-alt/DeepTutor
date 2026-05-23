"use client";

import { useCallback, useEffect, useState } from "react";
import {
  BarChart3,
  BookOpen,
  Loader2,
  RefreshCw,
  AlertCircle,
  TrendingUp,
  Target,
  FileText,
  Sparkles,
  X,
  ClipboardList,
} from "lucide-react";
import { useTranslation } from "react-i18next";
import SpaceSectionHeader from "@/components/space/SpaceSectionHeader";
import type { LucideIcon } from "lucide-react";

import {
  fetchMasterySummary,
  fetchWeakPoints,
  fetchWrongAnswers,
  fetchWeeklyStats,
  fetchMonthlyStats,
  generatePractice,
  generateExam,
  generateReport,
  type MasterySummary,
  type WeakPoint,
  type WrongAnswer,
  type PeriodStats,
  type PracticeQuestion,
} from "@/lib/platform-api";

// ── Learner ID ───────────────────────────────────────────────
// For now use a default — we'll wire to auth later.
const DEFAULT_LEARNER = "default";

// ── Stat Card ────────────────────────────────────────────────

function StatCard({
  icon: Icon,
  label,
  value,
  sub,
}: {
  icon: LucideIcon;
  label: string;
  value: string | number;
  sub?: string;
}) {
  return (
    <div className="flex items-center gap-3.5 rounded-xl border border-[var(--border)]/60 bg-[var(--card)] p-4 shadow-sm">
      <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg border border-[var(--border)]/60 bg-[var(--muted)]/40 text-[var(--foreground)]">
        <Icon size={18} strokeWidth={1.6} />
      </span>
      <div className="min-w-0">
        <p className="text-[13px] text-[var(--muted-foreground)]">{label}</p>
        <p className="text-xl font-semibold tracking-tight text-[var(--foreground)]">
          {value}
        </p>
        {sub && (
          <p className="text-[12px] text-[var(--muted-foreground)]">{sub}</p>
        )}
      </div>
    </div>
  );
}

// ── Day Bar (mini bar chart) ─────────────────────────────────

function DayBar({ day, count, max }: { day: string; count: number; max: number }) {
  const pct = max > 0 ? (count / max) * 100 : 0;
  return (
    <div className="flex flex-col items-center gap-1">
      <span className="text-[11px] font-medium text-[var(--muted-foreground)]">
        {count}
      </span>
      <div className="relative h-16 w-6 rounded-md bg-[var(--muted)]/50">
        <div
          className="absolute bottom-0 w-full rounded-md bg-[var(--primary)]/70 transition-all"
          style={{ height: `${pct}%`, minHeight: count > 0 ? "4px" : "0px" }}
        />
      </div>
      <span className="text-[10px] text-[var(--muted-foreground)]">{day}</span>
    </div>
  );
}

// ── Practice Modal ───────────────────────────────────────────

function PracticeModal({
  questions,
  kpId,
  onClose,
  onRegenerate,
  loading,
}: {
  questions: PracticeQuestion[];
  kpId: string;
  onClose: () => void;
  onRegenerate: () => void;
  loading: boolean;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div className="max-h-[80vh] w-full max-w-2xl overflow-y-auto rounded-2xl border border-[var(--border)] bg-[var(--card)] p-6 shadow-xl">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-lg font-semibold text-[var(--foreground)]">
            练习 — {kpId.split("/").pop()}
          </h2>
          <button onClick={onClose} className="rounded-lg p-1.5 hover:bg-[var(--muted)]/40">
            <X size={18} />
          </button>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="animate-spin" size={24} />
            <span className="ml-3 text-[14px] text-[var(--muted-foreground)]">生成练习中...</span>
          </div>
        ) : questions.length === 0 ? (
          <div className="flex flex-col items-center gap-3 py-12 text-[var(--muted-foreground)]">
            <AlertCircle size={24} />
            <p>没有生成练习题</p>
          </div>
        ) : (
          <div className="space-y-4">
            {questions.map((q, i) => (
              <div key={i} className="rounded-xl border border-[var(--border)]/60 bg-[var(--muted)]/20 p-4">
                <p className="mb-2 text-[14px] font-medium">
                  {i + 1}. {q.question}
                </p>
                {q.options && q.options.length > 0 && (
                  <ul className="mb-2 space-y-1 pl-4">
                    {q.options.map((opt, j) => (
                      <li key={j} className="text-[13px] text-[var(--foreground)]">
                        {opt}
                      </li>
                    ))}
                  </ul>
                )}
                <p className="text-[12px] text-[var(--muted-foreground)]">
                  答案: <span className="text-green-600">{q.answer}</span>
                </p>
                {q.explanation && (
                  <p className="mt-1 text-[12px] text-[var(--muted-foreground)]">
                    {q.explanation}
                  </p>
                )}
              </div>
            ))}
          </div>
        )}

        <div className="mt-4 flex justify-end gap-2">
          <button
            onClick={onClose}
            className="rounded-lg border border-[var(--border)] px-4 py-2 text-[13px] text-[var(--foreground)] hover:bg-[var(--muted)]/40"
          >
            关闭
          </button>
          <button
            onClick={onRegenerate}
            disabled={loading}
            className="rounded-lg bg-[var(--primary)] px-4 py-2 text-[13px] text-[var(--primary-foreground)] hover:opacity-90 disabled:opacity-50"
          >
            {loading ? "生成中..." : "重新生成"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Exam Modal ───────────────────────────────────────────────

function ExamModal({
  examText,
  title,
  kpCovered,
  onClose,
  onRegenerate,
  loading,
}: {
  examText: string;
  title: string;
  kpCovered: string[];
  onClose: () => void;
  onRegenerate: () => void;
  loading: boolean;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div className="max-h-[80vh] w-full max-w-3xl overflow-y-auto rounded-2xl border border-[var(--border)] bg-[var(--card)] p-6 shadow-xl">
        <div className="mb-4 flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-[var(--foreground)]">{title}</h2>
            {kpCovered.length > 0 && (
              <p className="mt-1 text-[12px] text-[var(--muted-foreground)]">
                覆盖知识点: {kpCovered.join(", ")}
              </p>
            )}
          </div>
          <button onClick={onClose} className="rounded-lg p-1.5 hover:bg-[var(--muted)]/40">
            <X size={18} />
          </button>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="animate-spin" size={24} />
            <span className="ml-3 text-[14px] text-[var(--muted-foreground)]">生成试卷中...</span>
          </div>
        ) : (
          <div className="prose prose-sm max-w-none whitespace-pre-wrap rounded-xl border border-[var(--border)]/60 bg-[var(--muted)]/20 p-4 text-[13px] leading-relaxed text-[var(--foreground)]">
            {examText}
          </div>
        )}

        <div className="mt-4 flex justify-end gap-2">
          <button
            onClick={onClose}
            className="rounded-lg border border-[var(--border)] px-4 py-2 text-[13px] text-[var(--foreground)] hover:bg-[var(--muted)]/40"
          >
            关闭
          </button>
          <button
            onClick={onRegenerate}
            disabled={loading}
            className="rounded-lg bg-[var(--primary)] px-4 py-2 text-[13px] text-[var(--primary-foreground)] hover:opacity-90 disabled:opacity-50"
          >
            {loading ? "生成中..." : "重新生成"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Report Modal ─────────────────────────────────────────────

function ReportModal({
  content,
  type,
  onClose,
}: {
  content: string;
  type: string;
  onClose: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div className="max-h-[80vh] w-full max-w-2xl overflow-y-auto rounded-2xl border border-[var(--border)] bg-[var(--card)] p-6 shadow-xl">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-lg font-semibold text-[var(--foreground)]">
            {type === "daily" ? "日报" : type === "weekly" ? "周报" : "月报"}
          </h2>
          <button onClick={onClose} className="rounded-lg p-1.5 hover:bg-[var(--muted)]/40">
            <X size={18} />
          </button>
        </div>
        <div className="whitespace-pre-wrap rounded-xl border border-[var(--border)]/60 bg-[var(--muted)]/20 p-4 text-[13px] leading-relaxed text-[var(--foreground)]">
          {content || "暂无学习记录"}
        </div>
        <div className="mt-4 flex justify-end">
          <button
            onClick={onClose}
            className="rounded-lg border border-[var(--border)] px-4 py-2 text-[13px] text-[var(--foreground)] hover:bg-[var(--muted)]/40"
          >
            关闭
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Main Dashboard Section ───────────────────────────────────

export default function LearningDashboardSection() {
  const { t } = useTranslation();

  // Data states
  const [summary, setSummary] = useState<MasterySummary | null>(null);
  const [weakPoints, setWeakPoints] = useState<WeakPoint[]>([]);
  const [wrongAnswers, setWrongAnswers] = useState<WrongAnswer[]>([]);
  const [weeklyStats, setWeeklyStats] = useState<PeriodStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Modal states
  const [practiceModal, setPracticeModal] = useState<{
    kpId: string;
    questions: PracticeQuestion[];
    loading: boolean;
  } | null>(null);
  const [examModal, setExamModal] = useState<{
    text: string;
    title: string;
    kps: string[];
    loading: boolean;
  } | null>(null);
  const [reportModal, setReportModal] = useState<{
    content: string;
    type: string;
  } | null>(null);

  // ── Data loading ──────────────────────────────────────────

  const loadAll = useCallback(async (force = false) => {
    setLoading(true);
    setError(null);
    try {
      const [sum, weak, wrong, weekly] = await Promise.all([
        fetchMasterySummary(DEFAULT_LEARNER),
        fetchWeakPoints(DEFAULT_LEARNER),
        fetchWrongAnswers(DEFAULT_LEARNER, undefined, 10),
        fetchWeeklyStats(DEFAULT_LEARNER),
      ]);
      setSummary(sum);
      setWeakPoints(weak);
      setWrongAnswers(wrong);
      setWeeklyStats(weekly);
    } catch (err) {
      setError(err instanceof Error ? err.message : "加载失败");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadAll(true);
  }, [loadAll]);

  // ── Handlers ──────────────────────────────────────────────

  const handlePractice = useCallback(
    async (kpId: string) => {
      setPracticeModal({ kpId, questions: [], loading: true });
      try {
        const result = await generatePractice(DEFAULT_LEARNER, kpId);
        setPracticeModal({ kpId, questions: result.questions, loading: false });
      } catch {
        setPracticeModal((prev) =>
          prev ? { ...prev, loading: false } : null,
        );
      }
    },
    [],
  );

  const handleExam = useCallback(async () => {
    setExamModal({ text: "", title: "生成中...", kps: [], loading: true });
    try {
      const result = await generateExam(DEFAULT_LEARNER);
      setExamModal({
        text: result.exam_text,
        title: result.title,
        kps: result.kp_covered,
        loading: false,
      });
    } catch {
      setExamModal((prev) =>
        prev ? { ...prev, text: "生成失败", loading: false } : null,
      );
    }
  }, []);

  const handleReport = useCallback(async (type: "daily" | "weekly" | "monthly") => {
    setReportModal({ content: "生成中...", type });
    try {
      const text = await generateReport(DEFAULT_LEARNER, type);
      setReportModal({ content: text || "暂无学习记录", type });
    } catch {
      setReportModal({ content: "生成失败", type });
    }
  }, []);

  // ── Render ────────────────────────────────────────────────

  // Loading state
  if (loading && !summary) {
    return (
      <div>
        <SpaceSectionHeader
          icon={BarChart3}
          title="学习进度"
          description="学习数据总览、薄弱知识点、错题本、练习与测试"
        />
        <div className="flex items-center justify-center py-20">
          <Loader2 className="animate-spin text-[var(--muted-foreground)]" size={24} />
          <span className="ml-3 text-[14px] text-[var(--muted-foreground)]">加载中...</span>
        </div>
      </div>
    );
  }

  // Error state
  if (error && !summary) {
    return (
      <div>
        <SpaceSectionHeader
          icon={BarChart3}
          title="学习进度"
          description="学习数据总览、薄弱知识点、错题本、练习与测试"
        />
        <div className="flex flex-col items-center gap-4 rounded-xl border border-red-200 bg-red-50 p-8 text-center">
          <AlertCircle className="text-red-500" size={28} />
          <p className="text-[14px] text-red-600">{error}</p>
          <button
            onClick={() => loadAll(true)}
            className="flex items-center gap-2 rounded-lg bg-red-500 px-4 py-2 text-[13px] text-white hover:bg-red-600"
          >
            <RefreshCw size={14} />
            重试
          </button>
        </div>
      </div>
    );
  }

  // Empty state
  const isEmpty = summary && summary.total_questions === 0;

  return (
    <div>
      <SpaceSectionHeader
        icon={BarChart3}
        title="学习进度"
        description="学习数据总览、薄弱知识点、错题本、练习与测试"
        action={
          <button
            onClick={() => loadAll(true)}
            className="flex items-center gap-1.5 rounded-lg border border-[var(--border)] px-3 py-1.5 text-[13px] text-[var(--foreground)] hover:bg-[var(--muted)]/40"
          >
            <RefreshCw size={14} />
            刷新
          </button>
        }
      />

      {isEmpty ? (
        <div className="flex flex-col items-center gap-4 rounded-xl border-2 border-dashed border-[var(--border)]/60 p-12 text-center">
          <BookOpen
            size={40}
            className="text-[var(--muted-foreground)]/50"
          />
          <p className="text-[15px] font-medium text-[var(--foreground)]">
            还没有学习记录
          </p>
          <p className="max-w-sm text-[13px] text-[var(--muted-foreground)]">
            通过微信发送作业或使用 DT 聊天开始学习，学习数据将在这里自动同步。
          </p>
        </div>
      ) : (
        <div className="space-y-6">
          {/* ── Overview Cards ── */}
          <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
            <StatCard
              icon={BookOpen}
              label="总答题数"
              value={summary?.total_questions ?? 0}
            />
            <StatCard
              icon={Target}
              label="正确率"
              value={`${summary?.accuracy ?? 0}%`}
            />
            <StatCard
              icon={TrendingUp}
              label="已掌握"
              value={summary?.mastered ?? 0}
              sub={`共 ${summary?.total_kp ?? 0} 个知识点`}
            />
            <StatCard
              icon={AlertCircle}
              label="薄弱点"
              value={weakPoints.length}
              sub={weakPoints.length > 0 ? "掌握度 < 60%" : "继续加油！"}
            />
          </div>

          {/* ── Weekly Trend ── */}
          {weeklyStats && weeklyStats.per_day.length > 0 && (
            <section className="rounded-xl border border-[var(--border)]/60 bg-[var(--card)] p-4 shadow-sm">
              <h3 className="mb-3 text-[14px] font-medium text-[var(--foreground)]">
                本周学习趋势（共 {weeklyStats.total} 题，正确率{" "}
                {weeklyStats.accuracy}%）
              </h3>
              <div className="flex items-end justify-around gap-1">
                {weeklyStats.per_day.map((d) => (
                  <DayBar
                    key={d.date}
                    day={d.date.slice(5)}
                    count={d.total}
                    max={Math.max(
                      ...weeklyStats.per_day.map((x) => x.total),
                      1,
                    )}
                  />
                ))}
              </div>
            </section>
          )}

          {/* ── Weak Points ── */}
          {weakPoints.length > 0 && (
            <section className="rounded-xl border border-[var(--border)]/60 bg-[var(--card)] p-4 shadow-sm">
              <div className="mb-3 flex items-center justify-between">
                <h3 className="text-[14px] font-medium text-[var(--foreground)]">
                  薄弱知识点（{weakPoints.length}）
                </h3>
                <button
                  onClick={handleExam}
                  className="flex items-center gap-1.5 rounded-lg bg-amber-500 px-3 py-1.5 text-[12px] text-white hover:bg-amber-600"
                >
                  <FileText size={13} />
                  生成强化试卷
                </button>
              </div>
              <div className="space-y-2">
                {weakPoints.map((wp) => (
                  <div
                    key={wp.kp_id}
                    className="flex items-center justify-between rounded-lg border border-[var(--border)]/60 bg-[var(--muted)]/20 p-3"
                  >
                    <div className="min-w-0">
                      <p className="truncate text-[13px] font-medium text-[var(--foreground)]">
                        {wp.kp_id.split("/").pop()}
                      </p>
                      <p className="text-[12px] text-[var(--muted-foreground)]">
                        掌握度 {Math.round(wp.level * 100)}% · 答 {wp.total} 对{" "}
                        {wp.correct}
                      </p>
                    </div>
                    <button
                      onClick={() => handlePractice(wp.kp_id)}
                      className="flex shrink-0 items-center gap-1 rounded-lg border border-[var(--border)] px-3 py-1.5 text-[12px] text-[var(--foreground)] hover:bg-[var(--muted)]/40"
                    >
                      <Sparkles size={13} />
                      生成练习
                    </button>
                  </div>
                ))}
              </div>
            </section>
          )}

          {/* ── Wrong Answers ── */}
          {wrongAnswers.length > 0 && (
            <section className="rounded-xl border border-[var(--border)]/60 bg-[var(--card)] p-4 shadow-sm">
              <h3 className="mb-3 flex items-center gap-2 text-[14px] font-medium text-[var(--foreground)]">
                <ClipboardList size={16} />
                最近错题（{wrongAnswers.length}）
              </h3>
              <div className="overflow-x-auto">
                <table className="w-full text-left text-[13px]">
                  <thead>
                    <tr className="border-b border-[var(--border)]/60 text-[12px] text-[var(--muted-foreground)]">
                      <th className="w-8 pb-2 font-medium">#</th>
                      <th className="pb-2 font-medium">题目</th>
                      <th className="pb-2 font-medium">你的答案</th>
                      <th className="pb-2 font-medium">正确答案</th>
                      <th className="pb-2 font-medium">知识点</th>
                    </tr>
                  </thead>
                  <tbody>
                    {wrongAnswers.map((wa, i) => (
                      <tr
                        key={i}
                        className="border-b border-[var(--border)]/30 last:border-0"
                      >
                        <td className="py-2.5 align-top text-[var(--muted-foreground)]">
                          {i + 1}
                        </td>
                        <td className="max-w-[200px] truncate py-2.5 align-top">
                          {wa.question}
                        </td>
                        <td className="py-2.5 align-top text-red-500">
                          {wa.user_answer || "-"}
                        </td>
                        <td className="py-2.5 align-top text-green-600">
                          {wa.correct_answer}
                        </td>
                        <td className="py-2.5 align-top text-[var(--muted-foreground)]">
                          {wa.kp_id.split("/").pop()}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          {/* ── Report Generation ── */}
          <section className="rounded-xl border border-[var(--border)]/60 bg-[var(--card)] p-4 shadow-sm">
            <h3 className="mb-3 text-[14px] font-medium text-[var(--foreground)]">
              学习报告
            </h3>
            <div className="flex flex-wrap gap-2">
              <button
                onClick={() => handleReport("daily")}
                className="flex items-center gap-1.5 rounded-lg border border-[var(--border)] px-3 py-1.5 text-[12px] text-[var(--foreground)] hover:bg-[var(--muted)]/40"
              >
                生成日报
              </button>
              <button
                onClick={() => handleReport("weekly")}
                className="flex items-center gap-1.5 rounded-lg border border-[var(--border)] px-3 py-1.5 text-[12px] text-[var(--foreground)] hover:bg-[var(--muted)]/40"
              >
                生成周报
              </button>
              <button
                onClick={() => handleReport("monthly")}
                className="flex items-center gap-1.5 rounded-lg border border-[var(--border)] px-3 py-1.5 text-[12px] text-[var(--foreground)] hover:bg-[var(--muted)]/40"
              >
                生成月报
              </button>
            </div>
          </section>
        </div>
      )}

      {/* ── Modals ── */}
      {practiceModal && (
        <PracticeModal
          questions={practiceModal.questions}
          kpId={practiceModal.kpId}
          loading={practiceModal.loading}
          onClose={() => setPracticeModal(null)}
          onRegenerate={() => handlePractice(practiceModal.kpId)}
        />
      )}

      {examModal && (
        <ExamModal
          examText={examModal.text}
          title={examModal.title}
          kpCovered={examModal.kps}
          loading={examModal.loading}
          onClose={() => setExamModal(null)}
          onRegenerate={handleExam}
        />
      )}

      {reportModal && (
        <ReportModal
          content={reportModal.content}
          type={reportModal.type}
          onClose={() => setReportModal(null)}
        />
      )}
    </div>
  );
}
